import pandas as pd


def merge_timeframes(dataframes: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Merge multiple timeframe DataFrames so that each row in the lower timeframe
    has the latest closed candle values from higher timeframes.

    Args:
        dataframes: dict where key is timeframe in minutes (str or int),
                    value is the corresponding DataFrame with a 'time' column.

    Returns:
        pd.DataFrame with aligned multi-timeframe data.
    """

    # Ensure keys are int and sort from smallest to largest TF
    tf_map = {int(tf): df.copy() for tf, df in dataframes.items()}
    timeframes = sorted(tf_map.keys())

    # Prepare all dataframes
    for tf in timeframes:
        df = tf_map[tf]
        df["time"] = pd.to_datetime(df["time"])

        # Add suffix to non-OHLC columns
        ohlc_cols = {"open", "high", "low", "close"}
        df.rename(columns={col: f"{col}_{tf}" for col in df.columns if col not in {"time"} and col not in ohlc_cols},
                  inplace=True)

        # For higher TFs, create merge_time as close time
        if tf != timeframes[0]:
            df["merge_time"] = df["time"] + pd.Timedelta(minutes=tf)
            # Drop OHLC for higher TFs
            df.drop(columns=["time", "open", "high", "low", "close"], inplace=True, errors="ignore")

        tf_map[tf] = df.sort_values("time" if tf == timeframes[0] else "merge_time")

    # Start with lowest TF dataframe
    merged_df = tf_map[timeframes[0]].copy()

    # Merge each higher TF
    for tf in timeframes[1:]:
        merged_df = pd.merge_asof(
            merged_df,
            tf_map[tf],
            left_on="time",
            right_on="merge_time",
            direction="backward"
        ).drop(columns=["merge_time"])

    main_timeframe = "_".join(dataframes.keys() )

    return {main_timeframe: merged_df}



# Example usage:
df_240 = pd.read_parquet(
    r'C:/Users/zak/Desktop/workspace/datalake/gold/xauusd/indicators/macd_16/xauusd_240_macd_16.parquet')
df_60 = pd.read_parquet(
    r'C:/Users/zak/Desktop/workspace/datalake/gold/xauusd/indicators/macd_16/xauusd_60_macd_16.parquet')

merged = merge_timeframes({
    "60": df_60,
    "240": df_240
})

