import asyncio
import random
import logging
import os
import signal
import time
import io
from dataclasses import dataclass
from typing import List, Optional, Tuple

import aiohttp
from aiohttp import web
import aiosqlite
import ccxt.async_support as ccxt
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg') # Use non-interactive backend for server-side chart generation


TOP_N = 5
MIN_GAIN_PCT = 40.0
TIMEFRAME = "5m"
CANDLE_LIMIT = 50
DEFAULT_TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]
TOP_REFRESH_SECONDS = 5 * 60
CHECK_INTERVAL_SECONDS = 120  # Spaced out even more
BB_PERIOD = 20
BB_STD_DEV = 2
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
PARALLEL_SYMBOL_WORKERS = 3  # Reduced further to be safe
MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "0"))
TIMEFRAMES = [tf.strip() for tf in os.getenv("TIMEFRAMES", "").split(",") if tf.strip()]
if not TIMEFRAMES:
    TIMEFRAMES = DEFAULT_TIMEFRAMES
MARKET_TYPE = (os.getenv("MARKET_TYPE", "futures").strip().lower() or "futures")
MAX_API_RETRIES = int(os.getenv("MAX_API_RETRIES", "5"))
BASE_RETRY_DELAY_SECONDS = float(os.getenv("BASE_RETRY_DELAY_SECONDS", "1.5"))  # Increased from 1.0
USE_RSI = os.getenv("USE_RSI", "no").strip().lower() == "yes"

ENABLE_COMMODITIES = os.getenv("ENABLE_COMMODITIES", "no").strip().lower() == "yes"
# Use MEXC Futures symbols for commodities
_default_commodities = "XAUT/USDT:USDT,SILVER/USDT:USDT,USOIL/USDT:USDT,UKOIL/USDT:USDT,NGAS/USDT:USDT,COPPER/USDT:USDT"
COMMODITY_SYMBOLS = [s.strip() for s in os.getenv("COMMODITY_SYMBOLS", _default_commodities).split(",") if s.strip()]


@dataclass
class SymbolStat:
    symbol: str
    gain_pct: float
    last_price: float
    quote_volume: float


def bollinger_bands(closes: List[float], period: int = BB_PERIOD, std_dev: int = BB_STD_DEV) -> Tuple[float, float, float]:
    window = closes[-period:]
    mean = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    std = variance ** 0.5
    upper = mean + std_dev * std
    lower = mean - std_dev * std
    return lower, mean, upper


def calculate_rsi(closes: List[float], period: int = RSI_PERIOD) -> float:
    if len(closes) <= period:
        return 50.0
    
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(diff))
            
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0:
        return 100.0
        
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
    if avg_loss == 0:
        return 100.0
        
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


class DatabaseManager:
    def __init__(self, db_path: str = "alerts.db"):
        self.db_path = db_path

    async def setup(self):
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            # Optimize for high-concurrency (Render/Linux)
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    alert_key TEXT,
                    message_text TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Migration: Check if message_text column exists, if not add it
            try:
                async with db.execute("SELECT message_text FROM alerts LIMIT 1") as cursor:
                    await cursor.fetchone()
            except aiosqlite.OperationalError:
                logging.info("Migrating database: adding message_text column")
                await db.execute("ALTER TABLE alerts ADD COLUMN message_text TEXT")
            
            await db.execute("CREATE INDEX IF NOT EXISTS idx_alerts_key ON alerts (symbol, alert_key)")
            await db.commit()

    async def is_alert_sent(self, symbol: str, alert_key: str) -> bool:
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            async with db.execute(
                "SELECT 1 FROM alerts WHERE symbol = ? AND alert_key = ? LIMIT 1",
                (symbol, alert_key)
            ) as cursor:
                return await cursor.fetchone() is not None

    async def save_alert(self, symbol: str, alert_key: str, message_text: str = ""):
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            await db.execute(
                "INSERT INTO alerts (symbol, alert_key, message_text) VALUES (?, ?, ?)",
                (symbol, alert_key, message_text)
            )
            await db.commit()

    async def get_last_alert(self) -> Optional[str]:
        async with aiosqlite.connect(self.db_path, timeout=30.0) as db:
            async with db.execute(
                "SELECT message_text FROM alerts ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None


class TopGainersBot:
    def __init__(self, telegram_token: str, telegram_chat_ids: List[str], exchange_id: str = "mexc") -> None:
        self.telegram_token = telegram_token
        self.telegram_chat_ids = [cid.strip() for cid in telegram_chat_ids if cid.strip()]
        self.exchange_id = exchange_id.lower().strip()
        self.db = DatabaseManager()
        exchange_cls = getattr(ccxt, self.exchange_id, None)
        if exchange_cls is None:
            raise ValueError(f"Unsupported exchange '{exchange_id}' in CCXT.")
        if MARKET_TYPE == "futures":
            self.exchange = exchange_cls({"enableRateLimit": True, "options": {"defaultType": "swap"}})
        else:
            self.exchange = exchange_cls({"enableRateLimit": True, "options": {"defaultType": "spot"}})
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.top_symbols: List[SymbolStat] = []
        self.commodity_symbols: List[SymbolStat] = [] # For commodities
        self.top_symbols_lock = asyncio.Lock()
        self.commodity_symbols_lock = asyncio.Lock() # For commodities
        self.stop_event = asyncio.Event()
        self.is_scanning = True  # Added flag to control scanning activity
        self.semaphore = asyncio.Semaphore(PARALLEL_SYMBOL_WORKERS)
        self.exchange_supported_timeframes: set[str] = set()

    @staticmethod
    def _is_tradable_market(market: dict) -> bool:
        if not market:
            return False
        quote = market.get("quote")
        settle = market.get("settle")
        if quote != "USDT" and settle != "USDT":
            return False
        info = market.get("info") or {}
        if MARKET_TYPE == "futures":
            if not (market.get("swap") or market.get("future") or market.get("contract")):
                return False
        else:
            if not market.get("spot"):
                return False
            # MEXC marks some symbols as spot but not tradable in regular spot UI.
            if "isSpotTradingAllowed" in info and not bool(info.get("isSpotTradingAllowed")):
                return False

        if not market.get("active", True):
            return False
        return True

    async def start(self) -> None:
        # Increase max_field_size for Yahoo Finance headers. 
        # Large cookies/headers from Yahoo can exceed the default 8KB.
        # Set to 64KB (65536) to be safe as seen in error logs (29899 bytes).
        self.http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            max_field_size=65536
        )
        await self.db.setup()
        await self.exchange.load_markets()
        self.exchange_supported_timeframes = set((self.exchange.timeframes or {}).keys())
        await self.refresh_top_symbols()
        
        # Build comprehensive startup message
        # Format commodity symbols for display (extracting the base name like GOLD, SILVER)
        comm_names = [s.split('/')[0].replace('(XAUT)', '').replace('(PAXG)', '') for s in COMMODITY_SYMBOLS]
        commodities_status = f"✅ Active ({len(COMMODITY_SYMBOLS)} symbols: {', '.join(comm_names)})" if ENABLE_COMMODITIES else "❌ Disabled"
        
        rsi_status = "✅ Active" if USE_RSI else "❌ Disabled"
        
        startup_msg = (
            "🤖 <b>Bot Started Successfully!</b>\n\n"
            "🔍 <b>Scanning Profile:</b>\n"
            f"• Crypto ({self.exchange_id.upper()}): Top {TOP_N} gainers with >{MIN_GAIN_PCT}% daily gain\n"
            f"• Commodities (MEXC Futures): {commodities_status}\n\n"
            "⏱️ <b>Intervals:</b>\n"
            f"• Refresh Top List: Every {TOP_REFRESH_SECONDS // 60} min\n"
            f"• Signal Check: Every {CHECK_INTERVAL_SECONDS // 60} min\n"
            f"• Timeframes (Crypto/Commodities): {', '.join(TIMEFRAMES)}\n\n"
            "📊 <b>Active Indicators:</b>\n"
            "• Bollinger Bands: ✅ Active\n"
            f"• RSI: {rsi_status}\n\n"
            "🚀 <i>The bot will now alert when deviations are detected!</i>"
        )
        
        await self.send_telegram(startup_msg)

        # 1. Start web server FIRST to satisfy Render health check quickly
        # This prevents Render from starting a second instance while this one is still "starting"
        web_server_task = asyncio.create_task(self._start_dummy_web_server())
        
        # Give web server a second to bind to the port
        await asyncio.sleep(2)

        # 2. Run bot tasks
        tasks = [
            self._top_symbols_scheduler(),
            self._signal_scheduler(),
            web_server_task
        ]
        
        # Optionally disable the listener to avoid 409 Conflict if multiple instances run
        if os.getenv("DISABLE_TELEGRAM_LISTENER", "no").strip().lower() != "yes":
            tasks.append(self._update_listener())
        else:
            logging.info("Telegram Listener is disabled via environment variable.")
        
        if ENABLE_COMMODITIES:
            logging.info("Commodities scanning enabled.")
            tasks.append(self._commodity_signal_scheduler())
            
        await asyncio.gather(*tasks)

    async def _start_dummy_web_server(self) -> None:
        """Start a simple HTTP server to satisfy Render's health checks (Free tier)."""
        app = web.Application()
        async def handle(request):
            return web.Response(text="Bot is running!")
        app.router.add_get('/', handle)
        
        # Render uses port 10000 by default for its health check
        port = int(os.getenv("PORT", "10000"))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        logging.info("Starting dummy web server on port %s", port)
        await site.start()
        
        # Wait for shutdown signal
        await self.stop_event.wait()
        await runner.cleanup()

    async def close(self) -> None:
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        await self.exchange.close()

    async def refresh_top_symbols(self) -> None:
        logging.info("Refreshing top gainers list...")
        tickers = await self._fetch_tickers_with_retry()

        stats: List[SymbolStat] = []
        for symbol, t in tickers.items():
            market = self.exchange.markets.get(symbol)
            if not self._is_tradable_market(market):
                continue
            if t.get("last") is None or t.get("percentage") is None:
                continue

            gain_pct = float(t["percentage"])
            if gain_pct < MIN_GAIN_PCT:
                continue

            last_price = float(t["last"])
            quote_volume = float(t.get("quoteVolume") or 0.0)

            stats.append(
                SymbolStat(
                    symbol=symbol,
                    gain_pct=gain_pct,
                    last_price=last_price,
                    quote_volume=quote_volume,
                )
            )

        stats.sort(key=lambda x: x.gain_pct, reverse=True)
        top = stats[:TOP_N]
        async with self.top_symbols_lock:
            self.top_symbols = top
        
        # Enhanced logging to help user see what is being watched
        top_symbols_str = ", ".join([f"{s.symbol} ({s.gain_pct:.1f}%)" for s in top])
        logging.info("Top list refreshed: %s symbols [%s]", len(top), top_symbols_str)
        if not top:
            logging.info("NO symbols currently meet the >%.1f%% gain threshold.", MIN_GAIN_PCT)

    def _get_mexc_link(self, symbol: str) -> str:
        # Converts symbol like BTC/USDT:USDT to BTC_USDT for MEXC
        # For spot: https://www.mexc.com/exchange/BTC_USDT
        # For futures: https://www.mexc.com/futures/BTC_USDT
        clean_symbol = symbol.replace("/", "_").split(":")[0]
        if MARKET_TYPE == "futures":
            return f"https://www.mexc.com/futures/{clean_symbol}"
        else:
            return f"https://www.mexc.com/exchange/{clean_symbol}"

    def _create_chart(self, symbol: str, closes: List[float], upper_band: float, lower_band: float, mean_band: float, timeframe: str = "") -> io.BytesIO:
        plt.figure(figsize=(10, 6))
        plt.plot(closes, label='Price', color='blue', linewidth=1.5)
        
        # Plot Bollinger Bands
        x = range(len(closes))
        plt.plot(x, [upper_band] * len(closes), label='Upper Band', color='red', linestyle='--', alpha=0.7)
        plt.plot(x, [mean_band] * len(closes), label='Middle Band', color='orange', linestyle=':', alpha=0.5)
        plt.plot(x, [lower_band] * len(closes), label='Lower Band', color='green', linestyle='--', alpha=0.7)
        
        plt.title(f"{symbol} - Bollinger Bands ({timeframe or TIMEFRAMES[0]})")
        plt.xlabel("Candles (Last 50)")
        plt.ylabel("Price")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        plt.close()
        return buf

    async def evaluate_symbol(self, symbol_stat: SymbolStat) -> None:
        async with self.semaphore:
            try:
                if symbol_stat.quote_volume < MIN_QUOTE_VOLUME_USDT:
                    return

                tf_metrics = []
                all_timeframes_match = True
                alert_key_parts: List[Tuple[str, int]] = []
                last_tf_bands = None # To store bands for the first timeframe for charting

                for timeframe in TIMEFRAMES:
                    # Small delay between timeframes to respect rate limits
                    await asyncio.sleep(0.5)
                    closes, last_candle_ts = await self._get_closes_for_timeframe(symbol_stat.symbol, timeframe, CANDLE_LIMIT)
                    if len(closes) < BB_PERIOD + 2:
                        all_timeframes_match = False
                        break

                    alert_key_parts.append((timeframe, last_candle_ts))
                    current_price = closes[-1]
                    lower_band, mean_band, upper_band = bollinger_bands(closes)
                    
                    # BB logic
                    bb_match = current_price > upper_band
                    
                    # RSI logic (if enabled)
                    rsi_match = True
                    current_rsi = None
                    if USE_RSI:
                        current_rsi = calculate_rsi(closes)
                        rsi_match = current_rsi > RSI_OVERBOUGHT
                    
                    tf_match = bb_match and rsi_match

                    if last_tf_bands is None:
                        last_tf_bands = (closes, lower_band, mean_band, upper_band)

                    tf_metrics.append((timeframe, current_price, upper_band, current_rsi, tf_match))
                    if not tf_match:
                        # Log partial matches to help user understand the strictness
                        if len(tf_metrics) > 1:
                            logging.info("Symbol %s matched %d/%d timeframes (stopped at %s)", 
                                         symbol_stat.symbol, len(tf_metrics)-1, len(TIMEFRAMES), timeframe)
                        all_timeframes_match = False
                        break

                if all_timeframes_match and tf_metrics:
                    logging.info("!!! SIGNAL DETECTED: %s matched all %d timeframes !!!", symbol_stat.symbol, len(TIMEFRAMES))
                    alert_key = str(tuple(sorted(alert_key_parts)))
                    if await self.db.is_alert_sent(symbol_stat.symbol, alert_key):
                        return

                    metrics_text_lines = []
                    for tf, price, upper, rsi, _ in tf_metrics:
                        line = f"• {tf}: price={price:.8f}, upper={upper:.8f}"
                        if rsi is not None:
                            line += f", RSI={rsi:.1f}"
                        metrics_text_lines.append(line)
                    
                    metrics_text = "\n".join(metrics_text_lines)
                    
                    exchange_link = self._get_mexc_link(symbol_stat.symbol)
                    
                    msg = (
                        "🚀 <b>SHORT signal detected (Multi-TF Confirmation)</b>\n"
                        f"Symbol: <b>{symbol_stat.symbol}</b>\n"
                        f"Market type: <i>{MARKET_TYPE}</i>\n"
                        f"24h Gain: <b>{symbol_stat.gain_pct:.2f}%</b>\n"
                        f"24h Quote Volume: <b>{symbol_stat.quote_volume:,.0f} USDT</b>\n"
                        f"Timeframes: {', '.join(TIMEFRAMES)}\n"
                        f"{metrics_text}\n\n"
                        f"🔗 <a href='{exchange_link}'>Click to Trade on MEXC</a>"
                    )
                    
                    await self.db.save_alert(symbol_stat.symbol, alert_key, message_text=msg)
                    
                    if last_tf_bands:
                        closes_list, l, m, u = last_tf_bands
                        chart_buf = self._create_chart(symbol_stat.symbol, closes_list[-50:], u, l, m, timeframe=TIMEFRAMES[0])
                        await self.send_telegram_photo(chart_buf, msg)
                    else:
                        await self.send_telegram(msg)
                        
                    logging.info("Signal(all TF): %s | gain=%0.2f", symbol_stat.symbol, symbol_stat.gain_pct)
            except Exception as exc:
                logging.warning("Failed symbol %s: %s", symbol_stat.symbol, exc)

    async def evaluate_commodity(self, symbol: str) -> None:
        """Evaluate a single commodity for BB/RSI signals on multiple timeframes using MEXC."""
        async with self.semaphore:
            try:
                # Check if the market exists in MEXC
                market = self.exchange.markets.get(symbol)
                if not market:
                    logging.warning("Commodity symbol %s not found in MEXC markets.", symbol)
                    return

                all_timeframes_match = True
                tf_metrics = []
                alert_key_parts = []
                last_tf_bands = None
                
                # Use TIMEFRAMES (crypto timeframes) since we are using MEXC
                for timeframe in TIMEFRAMES:
                    await asyncio.sleep(0.5) # Rate limiting
                    
                    closes, last_candle_ts = await self._get_closes_for_timeframe(symbol, timeframe, CANDLE_LIMIT)
                    
                    if len(closes) < BB_PERIOD + 2:
                        all_timeframes_match = False
                        break

                    current_price = closes[-1]
                    
                    lower_band, mean_band, upper_band = bollinger_bands(closes)
                    
                    bb_match = current_price > upper_band
                    
                    rsi_match = True
                    current_rsi = None
                    if USE_RSI:
                        current_rsi = calculate_rsi(closes)
                        rsi_match = current_rsi > RSI_OVERBOUGHT
                    
                    if bb_match and rsi_match:
                        tf_metrics.append((timeframe, current_price, upper_band, current_rsi))
                        alert_key_parts.append(f"{timeframe}_{last_candle_ts}")
                        if last_tf_bands is None:
                            last_tf_bands = (closes, lower_band, mean_band, upper_band)
                    else:
                        all_timeframes_match = False
                        break

                if all_timeframes_match and tf_metrics:
                    alert_key = f"commodity_{'_'.join(alert_key_parts)}"
                    if await self.db.is_alert_sent(symbol, alert_key):
                        return

                    logging.info("!!! COMMODITY SIGNAL DETECTED (Multi-TF): %s !!!", symbol)
                    
                    exchange_link = self._get_mexc_link(symbol)
                    
                    metrics_lines = []
                    for tf, price, upper, rsi in tf_metrics:
                        rsi_text = f", RSI={rsi:.1f}" if rsi is not None else ""
                        metrics_lines.append(f"• {tf}: Price={price:.4f}, Upper={upper:.4f}{rsi_text}")
                    
                    metrics_text = "\n".join(metrics_lines)
                    msg = (
                        "📉 <b>COMMODITY SHORT signal detected (Multi-TF Confirmation)</b>\n"
                        f"Symbol: <b>{symbol}</b>\n"
                        f"Type: <i>Commodity (MEXC Futures)</i>\n"
                        f"Confirmed on: {', '.join(TIMEFRAMES)}\n"
                        f"{metrics_text}\n\n"
                        f"🔗 <a href='{exchange_link}'>Trade on MEXC</a>"
                    )
                    
                    await self.db.save_alert(symbol, alert_key, message_text=msg)
                    
                    if last_tf_bands:
                        closes_list, l, m, u = last_tf_bands
                        chart_buf = self._create_chart(symbol, closes_list[-50:], u, l, m, timeframe=TIMEFRAMES[0])
                        await self.send_telegram_photo(chart_buf, msg)
                    else:
                        await self.send_telegram(msg)
                    
                    logging.info("Commodity Signal: %s | multi-tf confirmed", symbol)
            except Exception as exc:
                logging.warning("Failed commodity %s: %s", symbol, exc)

    async def _get_closes_for_timeframe(self, symbol: str, timeframe: str, limit: int) -> Tuple[List[float], int]:
        if timeframe in self.exchange_supported_timeframes:
            ohlcv = await self._fetch_ohlcv_with_retry(symbol, timeframe=timeframe, limit=limit)
            closes = [float(candle[4]) for candle in ohlcv]
            last_ts = int(ohlcv[-1][0]) if ohlcv else 0
            return closes, last_ts

        # MEXC does not expose 10m directly; build it from 5m candles.
        if timeframe == "10m" and "5m" in self.exchange_supported_timeframes:
            base = await self._fetch_ohlcv_with_retry(symbol, timeframe="5m", limit=limit * 2 + 4)
            grouped = []
            for i in range(0, len(base) - 1, 2):
                first = base[i]
                second = base[i + 1]
                if int(second[0]) <= int(first[0]):
                    continue
                grouped.append([int(second[0]), float(second[4])])
            if len(grouped) > limit:
                grouped = grouped[-limit:]
            closes = [item[1] for item in grouped]
            last_ts = int(grouped[-1][0]) if grouped else 0
            return closes, last_ts

        raise ValueError(f"Unsupported timeframe '{timeframe}' on {self.exchange_id.upper()}.")

    async def _fetch_tickers_with_retry(self) -> dict:
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_API_RETRIES + 1):
            try:
                return await self.exchange.fetch_tickers()
            except Exception as exc:
                last_exc = exc
                if attempt >= MAX_API_RETRIES:
                    break
                delay = BASE_RETRY_DELAY_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                await asyncio.sleep(delay)
        raise RuntimeError(f"fetch_tickers failed after {MAX_API_RETRIES} retries: {last_exc}")

    async def _fetch_ohlcv_with_retry(self, symbol: str, timeframe: str, limit: int) -> list:
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_API_RETRIES + 1):
            try:
                return await self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            except Exception as exc:
                last_exc = exc
                if attempt >= MAX_API_RETRIES:
                    break
                delay = BASE_RETRY_DELAY_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                await asyncio.sleep(delay)
        raise RuntimeError(
            f"fetch_ohlcv failed for {symbol} {timeframe} after {MAX_API_RETRIES} retries: {last_exc}"
        )

    async def check_signals(self) -> None:
        async with self.top_symbols_lock:
            snapshot = list(self.top_symbols)

        if not snapshot:
            logging.warning("Top symbols list is empty; skipping signal check.")
            return

        logging.info("Checking %s symbols...", len(snapshot))
        await asyncio.gather(*(self.evaluate_symbol(symbol_stat) for symbol_stat in snapshot))

    async def send_telegram(self, message: str) -> None:
        if not self.http_session or not self.telegram_chat_ids:
            return
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        for chat_id in self.telegram_chat_ids:
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
            }
            try:
                async with self.http_session.post(url, json=payload) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logging.error("Telegram send to %s failed [%s]: %s", chat_id, resp.status, body)
            except Exception as exc:
                logging.error("Telegram error for %s: %s", chat_id, exc)

    async def send_telegram_photo(self, photo_buf: io.BytesIO, caption: str) -> None:
        if not self.http_session or not self.telegram_chat_ids:
            return
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendPhoto"
        
        for chat_id in self.telegram_chat_ids:
            # We must reset the buffer position for each send
            photo_buf.seek(0)
            data = aiohttp.FormData()
            data.add_field("chat_id", chat_id)
            data.add_field("caption", caption)
            data.add_field("parse_mode", "HTML")
            data.add_field("photo", photo_buf, filename="chart.png", content_type="image/png")
            
            try:
                async with self.http_session.post(url, data=data) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logging.error("Telegram photo send to %s failed [%s]: %s", chat_id, resp.status, body)
            except Exception as exc:
                logging.error("Telegram photo error for %s: %s", chat_id, exc)

    async def _commodity_signal_scheduler(self) -> None:
        """Periodically check for signals on the predefined list of commodities."""
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                if self.is_scanning and COMMODITY_SYMBOLS:
                    logging.info("Checking %s commodities...", len(COMMODITY_SYMBOLS))
                    await asyncio.gather(*(self.evaluate_commodity(s) for s in COMMODITY_SYMBOLS))
            except Exception as exc:
                logging.exception("Commodity signal check failed: %s", exc)

            elapsed = time.monotonic() - started
            # Use the same interval as stocks for consistency
            wait_for = max(1, int(CHECK_INTERVAL_SECONDS - elapsed))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_for)
            except asyncio.TimeoutError:
                pass

    async def manual_check_symbol(self, raw_symbol: str, chat_id: str) -> None:
        """Manually checks a single symbol and sends detailed report to user."""
        symbol = raw_symbol.upper().strip()
        if "/" not in symbol:
            if MARKET_TYPE == "futures":
                symbol = f"{symbol}/USDT:USDT"
            else:
                symbol = f"{symbol}/USDT"
        
        url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        try:
            # Check if market exists
            market = self.exchange.markets.get(symbol)
            if not market:
                # Try one more variation for futures if not found
                if MARKET_TYPE == "futures" and ":USDT" not in symbol:
                    symbol = f"{symbol}:USDT"
                    market = self.exchange.markets.get(symbol)
                
                if not market:
                    await self.http_session.post(url_send, json={
                        "chat_id": chat_id, 
                        "text": f"❌ Symbol <b>{symbol}</b> not found on {self.exchange_id.upper()}.",
                        "parse_mode": "HTML"
                    })
                    return

            await self.http_session.post(url_send, json={
                "chat_id": chat_id, 
                "text": f"🔍 Checking <b>{symbol}</b> on {', '.join(TIMEFRAMES)}...",
                "parse_mode": "HTML"
            })

            ticker = await self.exchange.fetch_ticker(symbol)
            gain_pct = float(ticker.get("percentage") or 0.0)
            price = float(ticker.get("last") or 0.0)
            
            tf_results = []
            all_match = True
            
            for tf in TIMEFRAMES:
                closes, _ = await self._get_closes_for_timeframe(symbol, tf, CANDLE_LIMIT)
                if len(closes) < BB_PERIOD + 2:
                    tf_results.append(f"• {tf}: Not enough data")
                    all_match = False
                    continue
                
                lower, mean, upper = bollinger_bands(closes)
                current_rsi = calculate_rsi(closes) if USE_RSI else None
                
                bb_match = closes[-1] > upper
                rsi_match = (current_rsi > RSI_OVERBOUGHT) if USE_RSI else True
                match = bb_match and rsi_match
                
                status_emoji = "✅" if match else "❌"
                res_line = f"{status_emoji} <b>{tf}</b>: Price {closes[-1]:.6f} | Upper {upper:.6f}"
                if USE_RSI:
                    res_line += f" | RSI {current_rsi:.1f}"
                tf_results.append(res_line)
                
                if not match:
                    all_match = False

            final_status = "🚀 <b>SIGNAL MATCHED!</b>" if all_match else "⚠️ <b>No Signal</b>"
            report = (
                f"{final_status}\n\n"
                f"Symbol: <b>{symbol}</b>\n"
                f"Price: <b>{price:.6f}</b>\n"
                f"24h Gain: <b>{gain_pct:.2f}%</b>\n\n"
                "<b>Timeframe Details:</b>\n" + "\n".join(tf_results)
            )
            
            await self.http_session.post(url_send, json={
                "chat_id": chat_id, 
                "text": report,
                "parse_mode": "HTML"
            })

        except Exception as e:
            logging.error("Manual check error: %s", e)
            await self.http_session.post(url_send, json={
                "chat_id": chat_id, 
                "text": f"❌ Error checking {symbol}: {str(e)}"
            })

    async def _update_listener(self) -> None:
        """Listens for incoming messages to help user find Chat IDs."""
        # Wait a bit for other instances to shut down
        await asyncio.sleep(5)
        
        last_update_id = 0
        url = f"https://api.telegram.org/bot{self.telegram_token}/getUpdates"
        
        # Initial call to clear any old updates (skip messages sent while bot was offline)
        try:
            async with self.http_session.get(url, params={"offset": -1, "timeout": 1}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("result", [])
                    if results:
                        last_update_id = results[-1]["update_id"]
                        logging.info("Cleared old Telegram updates. Starting listener from update %s", last_update_id)
        except Exception as exc:
            logging.warning("Failed to clear initial Telegram updates: %s", exc)

        logging.info("ID Listener started. Send a message to the bot in any group to see its Chat ID.")
        
        while not self.stop_event.is_set():
            try:
                params = {"offset": last_update_id + 1, "timeout": 20}
                async with self.http_session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for update in data.get("result", []):
                            last_update_id = update["update_id"]
                            
                            # Log ALL updates for debugging
                            logging.info("--- TELEGRAM UPDATE DETECTED ---")
                            logging.info("Update Type: %s", list(update.keys())[-1])
                            
                            msg = update.get("message") or update.get("channel_post") or update.get("my_chat_member")
                            if msg:
                                chat = msg.get("chat", {})
                                chat_id = chat.get("id")
                                chat_title = chat.get("title", "Private Chat")
                                from_user = msg.get("from", {}).get("username", "Unknown")
                                text = msg.get("text", "").strip().lower()

                                logging.info("Chat Title: %s", chat_title)
                                logging.info("Chat ID: %s", chat_id)
                                logging.info("From: %s", from_user)
                                logging.info("Text: %s", text)
                                
                                if text == "/last":
                                    last_msg = await self.db.get_last_alert()
                                    if last_msg:
                                        # Use a smaller payload to send to just this requester
                                        url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                        payload = {
                                            "chat_id": chat_id,
                                            "text": f"📍 <b>Last Signal:</b>\n\n{last_msg}",
                                            "parse_mode": "HTML"
                                        }
                                        async with self.http_session.post(url_send, json=payload) as r:
                                            if r.status >= 400:
                                                logging.error("Failed to send /last response: %s", await r.text())
                                    else:
                                        url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                        payload = {"chat_id": chat_id, "text": "No signals stored in database yet."}
                                        await self.http_session.post(url_send, json=payload)
                                elif text == "/start":
                                    self.is_scanning = True
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    payload = {"chat_id": chat_id, "text": "✅ <b>Scanning Resumed!</b> The bot is now checking for signals.", "parse_mode": "HTML"}
                                    await self.http_session.post(url_send, json=payload)
                                    logging.info("Scanning resumed via Telegram command.")
                                elif text == "/stop":
                                    self.is_scanning = False
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    payload = {"chat_id": chat_id, "text": "🛑 <b>Scanning Paused!</b> The bot will stop sending alerts.", "parse_mode": "HTML"}
                                    await self.http_session.post(url_send, json=payload)
                                    logging.info("Scanning paused via Telegram command.")
                                elif text.startswith("/check"):
                                    parts = text.split()
                                    if len(parts) > 1:
                                        symbol_to_check = parts[1]
                                        asyncio.create_task(self.manual_check_symbol(symbol_to_check, chat_id))
                                    else:
                                        url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                        await self.http_session.post(url_send, json={
                                            "chat_id": chat_id, 
                                            "text": "⚠️ Please provide a symbol. Example: <code>/check btc</code>",
                                            "parse_mode": "HTML"
                                        })
                                elif text == "/help" or text == "/menu":
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    help_text = (
                                        "🤖 <b>Bot Menu:</b>\n\n"
                                        "▶️ /start - Resume scanning\n"
                                        "🛑 /stop - Pause scanning\n"
                                        "🔍 /check &lt;symbol&gt; - Manual signal check\n"
                                        "📍 /last - Show last signal details\n"
                                        "❓ /help - Show this menu\n\n"
                                        f"<i>Scanning crypto and commodities.</i>"
                                    )
                                    payload = {"chat_id": chat_id, "text": help_text, "parse_mode": "HTML"}
                                    await self.http_session.post(url_send, json=payload)

                                logging.info("--------------------------------")
                    else:
                        logging.error("Telegram Listener failed [%s]: %s", resp.status, await resp.text())
            except asyncio.TimeoutError:
                # Expected behavior for long polling, just ignore and continue
                pass
            except Exception as exc:
                logging.exception("Update listener exception: %s", exc)
            
            await asyncio.sleep(1)

    async def _top_symbols_scheduler(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                await self.refresh_top_symbols()
            except Exception as exc:
                logging.exception("Top list refresh failed: %s", exc)

            elapsed = time.monotonic() - started
            wait_for = max(1, int(TOP_REFRESH_SECONDS - elapsed))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_for)
            except asyncio.TimeoutError:
                pass

    async def _signal_scheduler(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                if self.is_scanning:
                    await self.check_signals()
                else:
                    logging.info("Scanning is currently disabled. Use /start to resume.")
            except Exception as exc:
                logging.exception("Signal check failed: %s", exc)

            elapsed = time.monotonic() - started
            wait_for = max(1, int(CHECK_INTERVAL_SECONDS - elapsed))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_for)
            except asyncio.TimeoutError:
                pass


async def main() -> None:
    pid_file = "bot.pid"
    
    # Check if bot is already running (Windows local check only)
    if os.name == "nt" and os.path.exists(pid_file):
        try:
            with open(pid_file, "r") as f:
                old_pid = int(f.read().strip())
            
            import subprocess
            output = subprocess.check_output(f'tasklist /FI "PID eq {old_pid}"', shell=True).decode()
            if str(old_pid) in output:
                print(f"❌ Bot is already running (PID: {old_pid})! Please stop it before starting a new instance.")
                return
            else:
                os.remove(pid_file)
        except Exception:
            if os.path.exists(pid_file):
                os.remove(pid_file)

    # Create PID file only on Windows
    if os.name == "nt":
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_ids_str = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    exchange_id = os.getenv("EXCHANGE_ID", "mexc").strip() or "mexc"
    if not token or not chat_ids_str:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID before running.")

    chat_ids = chat_ids_str.split(",")
    bot = TopGainersBot(token, chat_ids, exchange_id=exchange_id)
    loop = asyncio.get_running_loop()

    def _shutdown_handler() -> None:
        logging.info("Shutdown signal received.")
        bot.stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            try:
                loop.add_signal_handler(getattr(signal, sig_name), _shutdown_handler)
            except NotImplementedError:
                # Windows event loop may not support signal handlers.
                pass

    try:
        await bot.start()
    finally:
        if os.path.exists(pid_file):
            os.remove(pid_file)
        await bot.close()
        logging.info("Bot stopped.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(main())
