import pandas as pd
import numpy as np
import os

# -----------------------------------------
#   RSI FUNCTION
# -----------------------------------------
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# -----------------------------------------
#   FEATURE ENGINEERING FOR ONE DATAFRAME
# -----------------------------------------
def engineer_features(df):
    # --- Detect date column automatically ---
    date_col_candidates = ['date', 'Date', 'timestamp', 'datetime', 'time']
    date_col = None
    for col in date_col_candidates:
        if col in df.columns:
            date_col = col
            break
    if date_col is None:
        raise ValueError("No date/time column found in CSV!")

    df['date'] = pd.to_datetime(df[date_col])

    # --- Time-based features ---
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['weekday'] = df['date'].dt.weekday
    df['is_month_start'] = df['date'].dt.is_month_start.astype(int)
    df['is_month_end'] = df['date'].dt.is_month_end.astype(int)

    # --- Price & volume features ---
    df['high_low_range'] = df['high'] - df['low']
    df['average_price'] = df[['open','high','low','close']].mean(axis=1)
    df['volume_change'] = df['volume'].pct_change()

    # --- Lag & return features ---
    df['close_lag_1'] = df['close'].shift(1)
    df['close_lag_2'] = df['close'].shift(2)
    df['return'] = df['close'].pct_change()
    df['return_lag_1'] = df['return'].shift(1)

    # --- Rolling statistics ---
    df['rolling_mean_7'] = df['close'].rolling(7).mean()
    df['rolling_std_7'] = df['close'].rolling(7).std()
    df['rolling_mean_30'] = df['close'].rolling(30).mean()
    df['rolling_std_30'] = df['close'].rolling(30).std()

    # Moving averages
    for n in [14, 30, 50, 200]:
        df[f"ma{n}"] = df["close"].rolling(n).mean() / df["close"]

    # RSI
    for p in [14, 30, 50]:
        df[f"rsi{p}"] = compute_rsi(df["close"], period=p)

    # Rate of change
    df["roc14"] = df["close"].pct_change(14)

    # Volatility
    df["vol14"] = df["close"].pct_change().rolling(14).std()

    # --- Up/down day flags ---
    df['up_day'] = (df['close'] > df['open']).astype(int)
    df['down_day'] = (df['close'] < df['open']).astype(int)

    # Drop rows with NaNs created by rolling/lags
    df = df.dropna().reset_index(drop=True)

    return df


# -----------------------------------------
#   PIPELINE TO PROCESS ALL CSV FILES
# -----------------------------------------
def process_all(raw_root="data/", processed_root="data/processed"):
    # loop through subfolders
    for dataset in os.listdir(raw_root):
        dataset_path = os.path.join(raw_root, dataset)
        
        if not os.path.isdir(dataset_path):
            continue  # skip non-folders

        print(f"\n=== Processing dataset: {dataset} ===")

        # create processed subfolder
        out_folder = os.path.join(processed_root, dataset)
        os.makedirs(out_folder, exist_ok=True)

        # process each CSV in the dataset folder
        for file in os.listdir(dataset_path):
            if file.endswith(".csv"):
                infile = os.path.join(dataset_path, file)
                outfile = os.path.join(out_folder, file)

                print(f"Processing {infile} ...")

                df = pd.read_csv(infile)
                df = engineer_features(df)
                df = df.fillna(0)
                df.to_csv(outfile, index=False)

                print(f"Saved → {outfile}")


if __name__ == "__main__":
    process_all()
