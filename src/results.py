from dataclasses import dataclass
from typing import List
import pandas as pd
import logging
import os

from src.data_structure import BacktestResult, BacktestConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 8. RESULTS SAVING FUNCTIONS
# ============================================================================

def save_strategy_trades(result: BacktestResult, output_dir: str) -> None:
    """Save trades for a single strategy"""
    if result.trades_df.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    trades_path = os.path.join(output_dir, f"{result.strategy_name}_trades.parquet")
    result.trades_df.to_parquet(trades_path)
    logger.debug(f"Saved trades for {result.strategy_name}")


def save_all_results(results: List[BacktestResult], config: BacktestConfig) -> None:
    """Save all backtest results"""
    from collections import defaultdict

    # Group results by timeframe
    results_by_tf = defaultdict(list)
    for result in results:
        if result.success:
            results_by_tf[result.timeframe].append(result)

    # Save trades for each timeframe
    for timeframe, tf_results in results_by_tf.items():
        output_dir = os.path.join(config.save_path, config.indicator, config.strategy_type,  timeframe)

        for result in tf_results:
            save_strategy_trades(result, output_dir)

        logger.info(f"Saved {len(tf_results)} results for timeframe {timeframe}")


def save_summary_statistics(strategy_name:str, summary_df: pd.DataFrame, config: BacktestConfig) -> None:
    """Save summary statistics"""
    if summary_df.empty:
        return

    summary_path = os.path.join(
        config.save_path,
        config.indicator,
        config.strategy_type,
        "summary",
        f"summary_{config.strategy_type}_{config.indicator}_{strategy_name}.parquet"
    )

    os.makedirs(os.path.dirname(summary_path), exist_ok=True)
    summary_df.to_parquet(summary_path)

    profitable = len(summary_df[summary_df['net_profit'] > 0])
    logger.info(f"Saved summary: {len(summary_df)} strategies, {profitable} profitable")

def append_parquet_files(config: BacktestConfig):
    """
    Append all Parquet files in a folder into a single Parquet file.

    Args:
        input_folder (str): Path to the folder containing Parquet files.
        output_path (str): Path to save the combined Parquet file.
    """
    input_folder = os.path.join(
        config.save_path,
        config.indicator,
        config.strategy_type,
        "summary"
    )

    parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".parquet")]

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {input_folder}")

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
    output_path = os.path.join(
        config.save_path,
        config.indicator,
        config.strategy_type,
        f"summary_{config.strategy_type}_{config.indicator}.parquet"
    )
    combined_df.to_parquet(output_path, index=False)
    print(f"Combined Parquet saved to {output_path}")
