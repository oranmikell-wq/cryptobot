import asyncio
import ccxt.async_support as ccxt
import os

async def debug_symbol(symbol_name: str):
    exchange = ccxt.mexc({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
    try:
        await exchange.load_markets()
        
        # Try to find the exact symbol
        matches = [s for s in exchange.symbols if symbol_name.upper() in s]
        print(f"Matching symbols: {matches}")
        
        if not matches:
            print(f"Symbol {symbol_name} not found on MEXC.")
            return

        symbol = matches[0]
        ticker = await exchange.fetch_ticker(symbol)
        quote_volume = float(ticker.get('quoteVolume', 0))
        last_price = float(ticker.get('last', 0))
        percentage = float(ticker.get('percentage', 0))
        
        print(f"\n--- {symbol} Stats ---")
        print(f"24h Percentage: {percentage:.2f}%")
        print(f"24h Quote Volume: {quote_volume:,.0f} USDT")
        print(f"Last Price: {last_price:.8f}")
        
        # Check volume filter (2,000,000)
        min_vol = 2000000
        if quote_volume < min_vol:
            print(f"FAIL: Volume {quote_volume:,.0f} is LESS than {min_vol:,.0f} USDT.")
        else:
            print(f"PASS: Volume {quote_volume:,.0f} is GREATER than {min_vol:,.0f} USDT.")

        # Check Bollinger Bands (5m timeframe)
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=50)
        closes = [float(c[4]) for c in ohlcv]
        
        period = 20
        std_dev = 2
        window = closes[-period:]
        mean = sum(window) / period
        variance = sum((x - mean) ** 2 for x in window) / period
        std = variance ** 0.5
        upper = mean + std_dev * std
        
        print(f"\n--- Technicals (5m) ---")
        print(f"Current Price: {last_price:.8f}")
        print(f"Upper Bollinger Band: {upper:.8f}")
        
        if last_price > upper:
            print("PASS: Price is ABOVE Upper Band.")
        else:
            print(f"FAIL: Price is BELOW Upper Band. Difference: {upper - last_price:.8f}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    import sys
    symbol_arg = sys.argv[1] if len(sys.argv) > 1 else "BSB"
    asyncio.run(debug_symbol(symbol_arg))
