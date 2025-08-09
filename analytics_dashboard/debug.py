import pandas as pd
from pathlib import Path

symbol = "xauusd"

base_path = Path(fr"C:\Users\zak\Desktop\workspace\datalake\gold\{symbol}\backtest")
save_path = base_path / "merged_trades.parquet"

trades = {
    "bb": {
        "1": ["bb_20_1_5_1_0.2_0.8", "bb_25_1_5_1_0.2_0.8", "bb_30_1_5_1_0.2_0.8"],
        "5": ["bb_20_2_5_5_0_0.9", "bb_14_2_0_5_-0.1_0.9"]
    },
    "aroon": {
        "60": ["aroon_21_60"],
        "30": ["aroon_50_30"]
    },
    "ichimoku": {
        "60": ["ichimoku_12_20_45_20_60_tenken_kijun"],
        "30": ["ichimoku_7_35_45_30_30_cloud"]
    },
    "keltner": {
        "1": ["keltner_14_21_1_0_1_0.2_0.8", "keltner_20_21_1_0_1_0.2_0.8",
              "keltner_25_21_1_0_1_0.2_0.8", "keltner_50_21_1_0_1_0.2_0.8"],
    },
    "macd": {
        "60": ["macd_16_28_12_60", "macd_16_26_12_60"],
    },
    "rsi": {
        "240": ["rsi_13_240", "rsi_9_240", "rsi_11_240", "rsi_15_240"],
    },
    "sar": {
        "60": ["sar_1_1_60", "sar_1_25_60"],
        "240": ["sar_3_1_240"]
    },
    "stochrsi": {
        "240": ["stochrsi_21_14_4_3_240", "stochrsi_10_14_4_3_240", "stochrsi_14_14_4_3_240"],
    },
    "supertrend": {
        "240": ["supertrend_3_1_0_240", "supertrend_2_1_0_240",
                "supertrend_5_1_0_240", "supertrend_7_1_0_240"],
    },
    "ursi": {
        "240": ["ursi_5_240"],
        "60": ["ursi_15_60"],
        "30": ["ursi_24_30"]
    },
    "adx": {
        "240": ["adx_7_240", "adx_5_240"],
        "15": ["adx_50_15"],
        "60": ["adx_35_60"]
    },
}

all_dfs = []

for indicator, timeframes in trades.items():
    path_indicator = base_path / indicator / "simple"
    for timeframe, strategy_list in timeframes.items():
        for strategy_name in strategy_list:
            file_path = path_indicator / timeframe / f"{strategy_name}_trades.parquet"
            if file_path.exists():
                df = pd.read_parquet(file_path)
                df["indicator"] = indicator
                df["timeframe"] = timeframe
                df["strategy_name"] = strategy_name
                all_dfs.append(df)
            else:
                print(f"⚠ File not found: {file_path}")

# Merge all into one DataFrame
if all_dfs:
    df_result = pd.concat(all_dfs, ignore_index=True)
    df_result.to_parquet(save_path, index=False)
    print(f"✅ Saved merged DataFrame with {len(df_result)} rows to {save_path}")
else:
    print("❌ No data loaded.")
