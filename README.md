# Top 5 Gainers Short Alert Bot (MEXC default)

Async Python bot for crypto spot markets (MEXC by default) that:
- scans all `/USDT` pairs by 24h percentage change (minimum 40% gain),
- keeps the top 5 gainers list refreshed every 5 minutes,
- checks Bollinger + RSI signal every minute on 5m candles,
- sends Telegram alerts for SHORT conditions.

## Strategy

For each symbol in top 5:
- Fetch last 50 candles on `5m` timeframe.
- Calculate Bollinger Bands `(20, 2)`.
- Calculate RSI `(14)`.
- Trigger SHORT alert when:
  - `current_price > upper_band`
  - `RSI > 75`

## Requirements

- Python 3.10+
- Telegram bot token and chat ID

Install:

```bash
pip install -r requirements.txt
```

## Configuration

Set environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `MARKET_TYPE` (optional: `futures` or `spot`, default: `futures`)
- `TIMEFRAMES` (optional, comma-separated, default: `5m,15m,1h,4h,1d`)

PowerShell example:

```powershell
$env:TELEGRAM_BOT_TOKEN="123456:your_token"
$env:TELEGRAM_CHAT_ID="123456789"
$env:MARKET_TYPE="futures"
$env:TIMEFRAMES="1m,5m,15m"
python .\bot.py
```

When `TIMEFRAMES` has multiple values, the bot alerts only if the SHORT condition is true on all of them.

## Notes

- Uses `asyncio` + `aiohttp` and async CCXT.
- Parallel symbol processing is enabled with semaphore workers.
- MEXC public market data is used by default; API key is not required.
- You can change exchange by setting `EXCHANGE_ID` (for example: `binance`, `mexc`, `bybit`) if supported by CCXT.
