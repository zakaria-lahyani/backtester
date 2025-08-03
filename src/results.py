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
        output_dir = os.path.join(config.save_path, config.indicator, timeframe)

        for result in tf_results:
            save_strategy_trades(result, output_dir)

        logger.info(f"Saved {len(tf_results)} results for timeframe {timeframe}")


def save_summary_statistics(summary_df: pd.DataFrame, config: BacktestConfig) -> None:
    """Save summary statistics"""
    if summary_df.empty:
        return

    summary_path = os.path.join(
        config.save_path,
        config.indicator,
        f"summary_{config.indicator}.parquet"
    )

    os.makedirs(os.path.dirname(summary_path), exist_ok=True)
    summary_df.to_parquet(summary_path)

    profitable = len(summary_df[summary_df['net_profit'] > 0])
    logger.info(f"Saved summary: {len(summary_df)} strategies, {profitable} profitable")

