import pandas as pd
import os

def resample_ohlcv(df, timeframe):
    return df.resample(timeframe).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'tick_volume': 'sum'
    }).dropna()

symbol = "eurusd"

base_path = fr"C:\Users\zak\Desktop\workspace\datalake\raw\{symbol}"
dest_path = fr"C:\Users\zak\Desktop\workspace\datalake\raw\{symbol}"

# Ensure destination exists
os.makedirs(dest_path, exist_ok=True)

# Load data
df = pd.read_parquet(f"{base_path}/{symbol.upper()}_1.parquet")

# Ensure 'time' is a datetime index
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)

# Define resampling rules and output names
timeframes = {
    '5T': '5',
    '15T': '15',
    '30T': '30',
    '1h': '60',
    '4h': '240',
    '1D': 'DAILY',
    '1W': 'WEEKLY'
}

# Process and save each timeframe
for tf_rule, tf_name in timeframes.items():
    df_resampled = resample_ohlcv(df, tf_rule)
    output_file = os.path.join(dest_path, f"{symbol.upper()}_{tf_name}.parquet")
    df_resampled.to_parquet(output_file, engine="pyarrow")
    print(f"✅ Saved {len(df_resampled)} rows to {output_file}")
