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
import yfinance as yf
import pandas as pd
matplotlib.use('Agg') # Use non-interactive backend for server-side chart generation


TOP_N = 250
TIMEFRAME = "5m"
CANDLE_LIMIT = 50
TOP_REFRESH_SECONDS = 15 * 60
CHECK_INTERVAL_SECONDS = 120  # Spaced out even more
BB_PERIOD = 20
BB_STD_DEV = 2
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
PARALLEL_SYMBOL_WORKERS = 3  # Reduced further to be safe
MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "0"))
TIMEFRAMES = [tf.strip() for tf in os.getenv("TIMEFRAMES", TIMEFRAME).split(",") if tf.strip()]
MARKET_TYPE = (os.getenv("MARKET_TYPE", "futures").strip().lower() or "futures")
MAX_API_RETRIES = int(os.getenv("MAX_API_RETRIES", "5"))
BASE_RETRY_DELAY_SECONDS = float(os.getenv("BASE_RETRY_DELAY_SECONDS", "1.5"))  # Increased from 1.0
USE_RSI = os.getenv("USE_RSI", "no").strip().lower() == "yes"
ENABLE_STOCKS = os.getenv("ENABLE_STOCKS", "no").strip().lower() == "yes"
_raw_stock_tf = os.getenv("STOCK_TIMEFRAME", "5m").strip()
# Yahoo Finance supports only ONE interval string. If a list is provided, take the first one.
STOCK_TIMEFRAME = _raw_stock_tf.split(",")[0].strip() or "5m"


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
        self.stock_symbols: List[SymbolStat] = []
        self.top_symbols_lock = asyncio.Lock()
        self.stock_symbols_lock = asyncio.Lock()
        self.stock_api_lock = asyncio.Lock()  # Added lock specifically for sequential stock API calls
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
        # Increase max_field_size for Yahoo Finance headers
        self.http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            max_field_size=16384
        )
        await self.db.setup()
        await self.exchange.load_markets()
        self.exchange_supported_timeframes = set((self.exchange.timeframes or {}).keys())
        await self.refresh_top_symbols()
        await self.send_telegram(
            f"Bot started on {self.exchange_id.upper()}. Monitoring top {TOP_N} USDT gainers.\n"
            f"Market type: {MARKET_TYPE}\n"
            f"Top list refresh: every {TOP_REFRESH_SECONDS // 60}m\n"
            f"Bollinger/RSI checks: every {CHECK_INTERVAL_SECONDS // 60}m"
        )

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
        
        if ENABLE_STOCKS:
            logging.info("Stocks scanning enabled.")
            tasks.append(self._stock_symbols_scheduler())
            tasks.append(self._stock_signal_scheduler())
            
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
        logging.info("Top list refreshed: %s symbols", len(top))

    async def refresh_top_stocks(self) -> None:
        """Fetch US stock day gainers from Yahoo Finance."""
        logging.info("Refreshing top stocks list...")
        try:
            # Try a different URL that is more stable
            url = "https://finance.yahoo.com/markets/stocks/gainers/"
            try:
                # Mock a list of symbols if screener is not available or reliable
                # Yahoo Finance can be tricky with scraping, let's try a few standard ones too
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9"
                }
                async with self.http_session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        # Use a more specific extraction if read_html fails or gives wrong data
                        dfs = pd.read_html(html)
                        if dfs:
                            # Usually the first table
                            df = dfs[0]
                            # Clean column names if they have weird formatting
                            df.columns = [str(c).strip() for c in df.columns]
                            
                            stats = []
                            # Map columns based on potential names (Yahoo changes them often)
                            sym_col = next((c for c in df.columns if 'Symbol' in c or 'Ticker' in c), df.columns[0])
                            pct_col = next((c for c in df.columns if '%' in c or 'Change' in c), None)
                            price_col = next((c for c in df.columns if 'Price' in c), None)
                            vol_col = next((c for c in df.columns if 'Volume' in c), None)

                            for _, row in df.iterrows():
                                try:
                                    symbol = str(row[sym_col]).split()[0] # Take first part in case of "AAPL Apple Inc."
                                    if pct_col:
                                        pct_val = str(row[pct_col]).replace('%', '').replace('+', '').replace(',', '')
                                        pct_change = float(pct_val)
                                    else:
                                        pct_change = 0.0
                                    
                                    price = float(str(row[price_col]).replace(',', '')) if price_col else 0.0
                                    volume = float(str(row[vol_col]).replace('M', '000000').replace('K', '000').replace(',', '').replace('B', '000000000')) if vol_col else 0.0
                                    
                                    stats.append(SymbolStat(symbol=symbol, gain_pct=pct_change, last_price=price, quote_volume=volume))
                                except:
                                    continue
                            
                            if stats:
                                stats.sort(key=lambda x: x.gain_pct, reverse=True)
                                async with self.stock_symbols_lock:
                                    self.stock_symbols = stats[:TOP_N]
                                logging.info("Top stocks refreshed: %s symbols", len(self.stock_symbols))
                                return
            except Exception as e:
                logging.warning("Failed to scrape Yahoo day gainers: %s", e)
                
            # Fallback if scraping fails - keep it dynamic by adding some hot tickers
            async with self.stock_symbols_lock:
                fallback = ["NVDA", "TSLA", "AAPL", "AMD", "MSFT", "AMZN", "GOOGL", "META", "COIN", "MARA", "RIOT", "MSTR"]
                self.stock_symbols = [SymbolStat(s, 0, 0, 0) for s in fallback]
                logging.info("Using fallback stock list: %s", fallback)
        except Exception as exc:
            logging.error("refresh_top_stocks error: %s", exc)

    def _get_mexc_link(self, symbol: str) -> str:
        # Converts symbol like BTC/USDT:USDT to BTC_USDT for MEXC
        # For spot: https://www.mexc.com/exchange/BTC_USDT
        # For futures: https://www.mexc.com/futures/BTC_USDT
        clean_symbol = symbol.replace("/", "_").split(":")[0]
        if MARKET_TYPE == "futures":
            return f"https://www.mexc.com/futures/{clean_symbol}"
        else:
            return f"https://www.mexc.com/exchange/{clean_symbol}"

    def _get_yahoo_link(self, symbol: str) -> str:
        return f"https://finance.yahoo.com/quote/{symbol}"

    def _create_chart(self, symbol: str, closes: List[float], upper_band: float, lower_band: float, mean_band: float, is_stock: bool = False) -> io.BytesIO:
        plt.figure(figsize=(10, 6))
        plt.plot(closes, label='Price', color='blue', linewidth=1.5)
        
        # Plot Bollinger Bands
        x = range(len(closes))
        plt.plot(x, [upper_band] * len(closes), label='Upper Band', color='red', linestyle='--', alpha=0.7)
        plt.plot(x, [mean_band] * len(closes), label='Middle Band', color='orange', linestyle=':', alpha=0.5)
        plt.plot(x, [lower_band] * len(closes), label='Lower Band', color='green', linestyle='--', alpha=0.7)
        
        title_suffix = f"({STOCK_TIMEFRAME})" if is_stock else f"({TIMEFRAMES[0]})"
        plt.title(f"{symbol} - Bollinger Bands {title_suffix}")
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
                        # Optional: Log if it matched some timeframes but not all
                        if len(tf_metrics) >= 3:
                            logging.debug("Symbol %s matched %d/%d TFs (failed at %s)", 
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
                        chart_buf = self._create_chart(symbol_stat.symbol, *last_tf_bands)
                        await self.send_telegram_photo(chart_buf, msg)
                    else:
                        await self.send_telegram(msg)
                        
                    logging.info("Signal(all TF): %s | gain=%0.2f", symbol_stat.symbol, symbol_stat.gain_pct)
            except Exception as exc:
                logging.warning("Failed symbol %s: %s", symbol_stat.symbol, exc)

    async def evaluate_stock(self, symbol_stat: SymbolStat) -> None:
        """Evaluate a single stock for BB/RSI signals."""
        async with self.semaphore:
            # We use a separate lock for the API to ensure stock requests are strictly sequential
            # This is crucial for Yahoo Finance to avoid rate limiting
            async with self.stock_api_lock:
                try:
                    # Slow down requests to avoid Yahoo Finance rate limits
                    # 5.0 seconds with jitter is safer for Render data center IPs
                    await asyncio.sleep(5.0 + random.uniform(0, 2.0))
                    
                    # Redirect yfinance cache to /tmp to avoid permission/conflict errors in Render
                    try:
                        yf.set_tz_cache_location("/tmp/py-yfinance")
                    except:
                        pass
                        
                    # yfinance is synchronous, so we run it in a thread to not block the event loop
                    loop = asyncio.get_running_loop()
                    ticker = yf.Ticker(symbol_stat.symbol)
                    
                    # Fetch history
                    # We need at least BB_PERIOD + some buffer
                    # Ensure STOCK_TIMEFRAME is just a single string (interval)
                    interval = STOCK_TIMEFRAME
                    if not interval or "," in str(interval):
                        interval = "5m"
                        
                    hist = await loop.run_in_executor(None, lambda: ticker.history(period="5d", interval=interval))
                    
                    if hist.empty or len(hist) < BB_PERIOD + 2:
                        # Log specific reason if no data
                        if hist.empty:
                            logging.debug("Stock %s returned empty history for interval %s", symbol_stat.symbol, interval)
                        return

                closes = hist['Close'].tolist()
                current_price = closes[-1]
                last_candle_ts = int(hist.index[-1].timestamp())
                
                lower_band, mean_band, upper_band = bollinger_bands(closes)
                
                # BB logic
                bb_match = current_price > upper_band
                
                # RSI logic (if enabled)
                rsi_match = True
                current_rsi = None
                if USE_RSI:
                    current_rsi = calculate_rsi(closes)
                    rsi_match = current_rsi > RSI_OVERBOUGHT
                
                if bb_match and rsi_match:
                    alert_key = f"stock_{STOCK_TIMEFRAME}_{last_candle_ts}"
                    if await self.db.is_alert_sent(symbol_stat.symbol, alert_key):
                        return

                    logging.info("!!! STOCK SIGNAL DETECTED: %s !!!", symbol_stat.symbol)
                    
                    exchange_link = self._get_yahoo_link(symbol_stat.symbol)
                    
                    rsi_text = f", RSI={current_rsi:.1f}" if current_rsi is not None else ""
                    msg = (
                        "📈 <b>STOCK SHORT signal detected</b>\n"
                        f"Symbol: <b>{symbol_stat.symbol}</b>\n"
                        f"Type: <i>US Stock</i>\n"
                        f"Timeframe: {STOCK_TIMEFRAME}\n"
                        f"Price: {current_price:.2f}, Upper: {upper_band:.2f}{rsi_text}\n\n"
                        f"🔗 <a href='{exchange_link}'>View on Yahoo Finance</a>"
                    )
                    
                    await self.db.save_alert(symbol_stat.symbol, alert_key, message_text=msg)
                    
                    chart_buf = self._create_chart(symbol_stat.symbol, closes[-50:], upper_band, lower_band, mean_band, is_stock=True)
                    await self.send_telegram_photo(chart_buf, msg)
                    
                    logging.info("Stock Signal: %s | price=%0.2f", symbol_stat.symbol, current_price)
            except Exception as exc:
                logging.warning("Failed stock %s: %s", symbol_stat.symbol, exc)

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

    async def _stock_symbols_scheduler(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                await self.refresh_top_stocks()
            except Exception as exc:
                logging.exception("Stock list refresh failed: %s", exc)

            elapsed = time.monotonic() - started
            wait_for = max(1, int(TOP_REFRESH_SECONDS - elapsed))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_for)
            except asyncio.TimeoutError:
                pass

    async def _stock_signal_scheduler(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                if self.is_scanning:
                    async with self.stock_symbols_lock:
                        snapshot = list(self.stock_symbols)
                    
                    if snapshot:
                        logging.info("Checking %s stocks...", len(snapshot))
                        await asyncio.gather(*(self.evaluate_stock(s) for s in snapshot))
                else:
                    logging.info("Stock scanning is currently disabled. Use /start to resume.")
            except Exception as exc:
                logging.exception("Stock signal check failed: %s", exc)

            elapsed = time.monotonic() - started
            wait_for = max(1, int(CHECK_INTERVAL_SECONDS - elapsed))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_for)
            except asyncio.TimeoutError:
                pass

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
                                elif text == "/ha":
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    payload = {
                                        "chat_id": chat_id,
                                        "text": "חגי זה בן זונה גדול עושה ארגזים",
                                        "parse_mode": "HTML"
                                    }
                                    await self.http_session.post(url_send, json=payload)
                                elif text == "/start":
                                    self.is_scanning = True
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    payload = {"chat_id": chat_id, "text": "✅ <b>הבוט הופעל!</b> הסריקה מתחילה כעת.", "parse_mode": "HTML"}
                                    await self.http_session.post(url_send, json=payload)
                                    logging.info("Scanning resumed via Telegram command.")
                                elif text == "/stop":
                                    self.is_scanning = False
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    payload = {"chat_id": chat_id, "text": "🛑 <b>הבוט נעצר!</b> הסריקה הופסקה.", "parse_mode": "HTML"}
                                    await self.http_session.post(url_send, json=payload)
                                    logging.info("Scanning paused via Telegram command.")
                                elif text == "/help" or text == "/menu":
                                    url_send = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                                    help_text = (
                                        "🤖 <b>תפריט פקודות הבוט:</b>\n\n"
                                        "▶️ /start - הפעלת סריקת המטבעות\n"
                                        "🛑 /stop - עצירת סריקת המטבעות\n"
                                        "📍 /last - הצגת האיתות האחרון שנשלח\n"
                                        "💰 /ha - פקודת בונוס לחגי\n"
                                        "❓ /help - הצגת תפריט זה\n\n"
                                        "<i>הבוט סורק כעת 250 מטבעות ב-5 טווחי זמן.</i>"
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
                print(f"❌ הבוט כבר רץ (PID: {old_pid})! יש לעצור את הבוט לפני הרצה חדשה.")
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
