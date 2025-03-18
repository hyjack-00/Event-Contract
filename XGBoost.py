import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import argparse

# Define the list of intervals to load
INTERVALS = ["1m", "3m", "5m", "10m", "15m", "30m", "1h", "2h"]

def load_npz(symbol, interval, period):
    """
    Load npz file for a given symbol, interval, and period.
    Expected file name format: 
    klines_{symbol}_{interval}_{period}_until{YYYY_MM_DD}.npz
    """
    date_str = datetime.utcnow().strftime('%Y_%m_%d')
    filename = f"klines_{symbol}_{interval}_{period}_until{date_str}.npz"
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
        return None
    data = np.load(filename, allow_pickle=True)
    # Create a DataFrame using the stored arrays.
    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "quote_asset_volume": data["quote_asset_volume"],
        "num_trades": data["num_trades"],
        "taker_buy_quote_volume": data["taker_buy_quote_volume"],
    })
    # Convert numeric columns to float and timestamp to datetime
    numeric_cols = df.columns.drop("timestamp")
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Add an "interval" column indicating the data source
    df["interval"] = interval
    return df

def compute_features_for_interval(df, sample_time, window_duration, interval_name):
    """
    Compute aggregated features for the given interval over a window ending at sample_time.
    Aggregations include: mean, std, last, min, and max for each numeric column.
    """
    start_time = sample_time - window_duration
    # Filter rows within the window [start_time, sample_time]
    window_df = df[(df["timestamp"] >= start_time) & (df["timestamp"] <= sample_time)]
    features = {}
    # List the columns to aggregate. (Ensure these columns exist in your data)
    cols = ["open", "high", "low", "close", "volume", 
            "quote_asset_volume", "num_trades", "taker_buy_quote_volume"]
    if window_df.empty:
        # Fill with NaNs if no data is available
        for col in cols:
            features[f"{interval_name}_{col}_mean"] = np.nan
            features[f"{interval_name}_{col}_std"] = np.nan
            features[f"{interval_name}_{col}_last"] = np.nan
            features[f"{interval_name}_{col}_min"] = np.nan
            features[f"{interval_name}_{col}_max"] = np.nan
        return features
    for col in cols:
        if col in window_df.columns:
            features[f"{interval_name}_{col}_mean"] = window_df[col].mean()
            features[f"{interval_name}_{col}_std"] = window_df[col].std()
            features[f"{interval_name}_{col}_last"] = window_df[col].iloc[-1]
            features[f"{interval_name}_{col}_min"] = window_df[col].min()
            features[f"{interval_name}_{col}_max"] = window_df[col].max()
        else:
            features[f"{interval_name}_{col}_mean"] = np.nan
            features[f"{interval_name}_{col}_std"] = np.nan
            features[f"{interval_name}_{col}_last"] = np.nan
            features[f"{interval_name}_{col}_min"] = np.nan
            features[f"{interval_name}_{col}_max"] = np.nan
    return features

def generate_training_data(data_dict, base_interval="1h", window_days=3, horizon="1h"):
    """
    Generate training samples.
    For each sample time (from the base interval data), compute features over the past window_days
    from every interval. The label is 1 if the close price at (sample_time + horizon) is higher than
    the current close price, else 0.
    """
    base_df = data_dict[base_interval].copy().sort_values("timestamp")
    
    # Determine prediction horizon as a timedelta
    if horizon.endswith("h"):
        horizon_td = timedelta(hours=int(horizon[:-1]))
    elif horizon.endswith("min"):
        horizon_td = timedelta(minutes=int(horizon[:-3]))
    else:
        raise ValueError("Invalid horizon format. Use e.g., '1h' or '10min'")
        
    window_duration = timedelta(days=window_days)
    
    feature_list = []
    label_list = []
    sample_times = []
    
    # Loop over base_df rows (each row is a sample time)
    for idx, row in base_df.iterrows():
        sample_time = row["timestamp"]
        future_time = sample_time + horizon_td
        # Find the closest future row (using the base interval timeline)
        future_rows = base_df[base_df["timestamp"] >= future_time]
        if future_rows.empty:
            continue
        future_row = future_rows.iloc[0]
        # Label: 1 if future close > current close, else 0
        label = 1 if future_row["close"] > row["close"] else 0
        
        sample_features = {}
        # Compute features for every interval for the 3-day window ending at sample_time
        for interval, df in data_dict.items():
            feats = compute_features_for_interval(df, sample_time, window_duration, interval)
            sample_features.update(feats)
        
        # Optionally, skip samples with any missing feature
        if any(np.isnan(list(sample_features.values()))):
            continue
        
        feature_list.append(sample_features)
        label_list.append(label)
        sample_times.append(sample_time)
        
    feature_df = pd.DataFrame(feature_list)
    labels = np.array(label_list)
    return feature_df, labels, sample_times

def main(args):
    symbol = args.symbol
    period = args.period
    # Split the comma-separated list of intervals
    intervals = args.intervals.split(",") if args.intervals else INTERVALS
    
    # Load npz data for each interval into a dictionary
    data_dict = {}
    for interval in intervals:
        df = load_npz(symbol, interval, period)
        if df is not None:
            data_dict[interval] = df
        else:
            print(f"Data for interval {interval} not loaded.")
    
    # Ensure base interval data is available for label generation
    base_interval = "1h"
    if base_interval not in data_dict:
        raise ValueError(f"Base interval data ({base_interval}) is required for label generation.")
    
    print("Generating training data...")
    X, y, sample_times = generate_training_data(data_dict, base_interval=base_interval, window_days=3, horizon=args.horizon)
    
    print(f"Generated {X.shape[0]} samples with {X.shape[1]} features.")
    
    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # Train an XGBoost classifier
    model = xgb.XGBClassifier(n_estimators=100, max_depth=5, learning_rate=0.1, 
                              use_label_encoder=False, eval_metric='logloss')
    model.fit(X_train, y_train)
    
    # Evaluate the model
    y_pred = model.predict(X_test)
    print("Classification Report:")
    print(classification_report(y_test, y_pred))
    
    # Save the trained model
    model.save_model("xgb_model.json")
    print("Model saved as xgb_model.json")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XGBoost model for BTC price movement prediction")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair symbol")
    parser.add_argument("--period", type=str, default="365d", help="Data period (e.g., '365d')")
    parser.add_argument("--intervals", type=str, default="1m,3m,5m,10m,15m,30m,1h,2h", 
                        help="Comma-separated list of intervals")
    parser.add_argument("--horizon", type=str, default="1h", help="Prediction horizon (e.g., '1h' or '10min')")
    args = parser.parse_args()
    main(args)
