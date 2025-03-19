import argparse
import os
import time
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from ..utils import period_to_milliseconds

# Binance Futures API URL
BASE_URL = "https://fapi.binance.com/fapi/v1/klines"

# Default intervals on Binance
INTERVALS = [
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"
]

# Fetch historical K-lines
def fetch_klines(symbol, interval, period):
    end_time = int(time.time() * 1000)  # Current time in ms
    start_time = end_time - period_to_milliseconds(period)
    print(f"Fetching {symbol} {interval} K-lines from {pd.to_datetime(start_time, unit='ms')} to {pd.to_datetime(end_time, unit='ms')}...")
    
    all_data = []
    try:
        while start_time < end_time:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_time,
                "endTime": end_time,
                "limit": 1000
            }

            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            # print(data)
            if not data:
                break

            all_data.extend(data)
            start_time = data[-1][0] + 1
            if len(all_data) % 30000 == 0:
                print(f"Received {len(all_data)} K-lines")

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data, columns=[
        "timestamp", "open", "high", "low", "close", "volume", 
        "close_time", "quote_asset_volume", "num_trades", 
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    print(f"Received {len(df)} K-lines")
    print(f"First item: {df.iloc[0]}")
    
    # rm "close_time", for only recording open_time (timestamp)
    # rm "taker_buy_base_volume", for quote_volume is enough
    # rm "ignore"
    df = df[["timestamp", "open", "high", "low", "close", "volume", 
        "quote_asset_volume", "num_trades", "taker_buy_quote_volume"]]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    
    return df


# Save data as .npz
def save_data(symbol, interval, period, df):
    date_str = datetime.utcnow().strftime('%Y_%m_%d')
    filename = f"klines_{symbol}_{interval}_{period}_until{date_str}.npz"
    np.savez(filename, timestamp=df["timestamp"].values, 
             open=df["open"].astype(float).values, 
             high=df["high"].astype(float).values, 
             low=df["low"].astype(float).values, 
             close=df["close"].astype(float).values, 
             volume=df["volume"].astype(float).values)
    print(f"Saved: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair symbol")
    parser.add_argument("--interval", type=str, default=None, help="K-line interval (e.g., '1m', '5m')")
    parser.add_argument("--period", type=str, default="365d", help="Data period (e.g., '365d', '48h')")
    args = parser.parse_args()

    # If no interval is provided, fetch all
    intervals = [args.interval] if args.interval else INTERVALS
    
    for interval in intervals:
        df = fetch_klines(args.symbol, interval, args.period)
        save_data(args.symbol, interval, args.period, df)
