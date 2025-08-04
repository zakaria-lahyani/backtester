import pandas as pd
import glob
import os

# Define header names
cols = ["time", "open", "high", "low", "close", "tick_volume"]

base_path = r"C:\Users\zak\Desktop\workspace\datalake\input\eurusd"
dest_path = r"C:\Users\zak\Desktop\workspace\datalake\raw\eurusd"

# Ensure destination folder exists
os.makedirs(dest_path, exist_ok=True)

# Find all EURUSD_M1_YYYY.csv files
files = sorted(glob.glob(f"{base_path}/*EURUSD_M1_*.csv"))

# Load and concatenate
df = pd.concat(
    (pd.read_csv(f, names=cols, header=None, sep=";") for f in files),
    ignore_index=True
)

# Remove duplicates based on "time" column, keeping the first occurrence
df = df.drop_duplicates(subset=["time"], keep="first")

# Save as Parquet file
output_file = os.path.join(dest_path, "EURUSD_1.parquet")
df.to_parquet(output_file, engine="pyarrow", index=False)

print(f"✅ Saved {len(df)} rows to {output_file}")
