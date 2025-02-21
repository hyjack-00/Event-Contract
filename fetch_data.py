import requests
import pandas as pd
import argparse
import time
from datetime import datetime, timedelta
import numpy as np

# Binance API 获取 K 线数据
BASE_URL = "https://api.binance.com/api/v3/klines"

# 预定义的时间间隔
INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h"]

# 获取 K 线数据
def fetch_klines(symbol="BTCUSDT", interval="1h", lookback_days=180, end_time=None):
    """
    从币安获取指定 interval 的 K 线数据
    :param symbol: 交易对, 如 "BTCUSDT"
    :param interval: K 线时间间隔, 如 "1m", "5m", "1h", "1d"
    :param lookback_days: 回溯天数, 默认 180 天
    :param end_time: 结束时间, 默认为当前时间
    :return: Pandas DataFrame
    """
    if end_time is None:
        end_time = int(time.time() * 1000)  # 当前时间戳 (ms)
    else:
        end_time = int(pd.to_datetime(end_time).timestamp() * 1000)
    
    start_time = end_time - lookback_days * 24 * 60 * 60 * 1000
    
    all_data = []
    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        
        if not data:
            break
        
        all_data.extend(data)
        start_time = data[-1][0] + 1  # 更新起始时间
        time.sleep(0.5)  # 避免 API 速率限制
    
    df = pd.DataFrame(all_data, columns=[
        "timestamp", "open", "high", "low", "close", "volume", "close_time", 
        "quote_asset_volume", "number_of_trades", "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
    ])
    
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    # 仅对数值型列进行 float 转换
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    
    return df


# 批量获取所有 interval 的数据
def fetch_all_intervals(symbol="BTCUSDT", lookback_days=30, end_time=None, save_path="btc_klines.npz"):
    """
    获取所有 interval K 线数据，并保存为 npz 文件
    """
    data_dict = {}
    for interval in INTERVALS:
        print(f"Fetching {interval} data...")
        data_dict[interval] = fetch_klines(symbol, interval, lookback_days, end_time)
    
    # 转换为 NumPy 数组并存储
    np.savez(save_path, **{k: v.to_numpy() for k, v in data_dict.items()})
    print(f"Data saved to {save_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Kline data from Binance")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair")
    parser.add_argument("--lookback_days", type=int, default=30, help="Lookback days")
    parser.add_argument("--end_time", type=str, default=None, help="End time")
    parser.add_argument("--save_path", type=str, default="btc_klines.npz", help="Save path")
    args = parser.parse_args()

    fetch_all_intervals(args.symbol, args.lookback_days, args.end_time, args.save_path)
