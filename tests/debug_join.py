import os
import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)

def append_parquet_files(input_folder: str, output_path: str):
    """
    Append all Parquet files in a folder into a single Parquet file.

    Args:
        input_folder (str): Path to the folder containing Parquet files.
        output_path (str): Path to save the combined Parquet file.
    """
    parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".parquet")]

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {input_folder}")

    print(f"reding data")
    df_list = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            df_list.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if not df_list:
        raise ValueError("No valid Parquet files could be read.")

    combined_df = pd.concat(df_list, ignore_index=True)

    # Save as Parquet
    combined_df.to_parquet(output_path, index=False)
    print(f"Combined Parquet saved to {output_path}")

base_path = r'C:\Users\zak\Desktop\workspace\datalake\gold\xauusd\backtest\macd'
summary_folder = 'summary'

# append_parquet_files(f"{base_path}/summary", f"{base_path}/summary.parquet")

df_summary_combined = pd.read_parquet(rf"{base_path}\combined/summary.parquet")
df_summary_simple = pd.read_parquet(rf"{base_path}\simple/summary_simple_macd.parquet")

# print(df_summary_combined.sort_values("net_profit", ascending=False).head(50))
# print(df_summary_simple.sort_values("net_profit", ascending=False).head(50))

name_240 = "10_26_12_240"
name_60 = "16_20_9_60"

df_combined = df_summary_combined[df_summary_combined["strategy_name"] == f"macd_{name_240}_{name_60}"]
df_simple = df_summary_simple[df_summary_simple["strategy_name"].isin([f"macd_{name_240}", f"macd_{name_60}" ])]

print(df_combined)
print("------------------------------------------------------------------------")
print(" ")
print("------------------------------------------------------------------------")
print(df_simple)
# print(df_simple)

