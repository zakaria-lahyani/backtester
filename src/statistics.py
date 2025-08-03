from typing import List
import pandas as pd
import logging

from src.data_structure import BacktestResult

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 7. SUMMARY AND STATISTICS FUNCTIONS
# ============================================================================

def compute_strategy_statistics(
        trades_df: pd.DataFrame,
        stats_df: pd.DataFrame,
        strategy_name: str,
        timeframe: str
) -> pd.DataFrame:
    """Compute detailed statistics for a strategy"""
    try:
        import numpy as np

        total_trades = stats_df["Total Trades"]
        win_rate_pct = stats_df["Win Rate [%]"]
        best_trade_pct = stats_df["Best Trade [%]"]
        worst_trade_pct = stats_df["Worst Trade [%]"]
        avg_winning_pct = stats_df["Avg Winning Trade [%]"]
        avg_losing_pct = stats_df["Avg Losing Trade [%]"]
        profit_factor = stats_df["Profit Factor"]

        wins = trades_df[trades_df['pnl'] > 0].shape[0]
        losses = trades_df[trades_df['pnl'] < 0].shape[0]
        avg_win = trades_df.loc[trades_df['pnl'] > 0, 'pnl'].mean() if wins > 0 else 0
        avg_loss = trades_df.loc[trades_df['pnl'] < 0, 'pnl'].mean() if losses > 0 else 0

        largest_win = trades_df['pnl'].max() if total_trades > 0 else 0
        largest_loss = trades_df['pnl'].min() if total_trades > 0 else 0

        avg_profit = trades_df['pnl'].mean() if total_trades > 0 else 0
        total_net_profit = trades_df['pnl'].sum()
        avg_duration = trades_df["duration"].mean() if total_trades > 0 else 0

        expectancy = avg_profit

        # Calculate max drawdown
        max_dd, max_dd_pct, dd_start, dd_end = calculate_max_drawdown(trades_df, "exit_timestamp", "pnl")
        return_to_dd = total_net_profit / abs(max_dd) if max_dd != 0 else np.inf

        stats_dict = {
            "strategy_name": strategy_name,
            "timeframe": timeframe,
            "start": stats_df["Start"],
            "end": stats_df["End"],
            "period": stats_df["Period"],
            "benchmark_return_pct": stats_df["Benchmark Return [%]"],
            "nbr_trades": total_trades,
            "winrate": round(win_rate_pct, 2),
            "avg_trade_return": round(avg_profit, 2),
            "avg_trade_duration": round(avg_duration, 2),
            "profit_factor": round(profit_factor, 2),
            "expectancy": round(expectancy, 2),
            "avg_win": round(avg_win, 2),
            "avg_win_pct": round(avg_winning_pct, 2),
            "avg_loss": round(avg_loss, 2),
            "avg_loss_pct": round(avg_losing_pct, 2),
            "best_trade": round(largest_win, 2),
            "best_trade_pct": round(best_trade_pct, 2),
            "worst_trade": round(largest_loss, 2),
            "worst_trade_pct": round(worst_trade_pct, 2),
            "drawdown": round(max_dd, 2),
            "drawdown_start": dd_start,
            "drawdown_end": dd_end,
            "drawdown_pct": round(stats_df["Max Drawdown [%]"], 2) * 100,
            "return_to_dd": round(return_to_dd, 2),
            "sharpe": round(stats_df["Sharpe Ratio"], 2),
            "calmar": round(stats_df["Calmar Ratio"], 2),
            "omega": round(stats_df["Omega Ratio"], 2),
            "sortino": round(stats_df["Sortino Ratio"], 2),
            "net_profit": round(total_net_profit, 2),
        }

        return pd.DataFrame([stats_dict])

    except Exception as e:
        logger.error(f"Failed to compute statistics for {strategy_name}: {e}")
        return pd.DataFrame()


def calculate_max_drawdown(df: pd.DataFrame, date_col: str, profit_col: str) -> tuple:
    """Calculate maximum drawdown"""
    import numpy as np

    # Sort by date
    df = df.sort_values(by=date_col).copy()
    df['equity'] = df[profit_col].cumsum()

    # Calculate running max
    df['peak'] = df['equity'].cummax()

    # Calculate drawdown (in absolute terms)
    df['drawdown'] = df['equity'] - df['peak']

    # Find max drawdown row (most negative drawdown)
    dd_row = df.loc[df['drawdown'].idxmin()]
    dd_end = dd_row[date_col]
    max_dd = dd_row['drawdown']

    # Find peak before the drawdown
    peak_df = df[df[date_col] <= dd_end]
    peak_row = peak_df.loc[peak_df['equity'].idxmax()]
    dd_start = peak_row[date_col]
    peak_equity = peak_row['equity']

    # Drawdown percentage
    max_dd_pct = (max_dd / peak_equity) * 100 if peak_equity != 0 else 0

    return max_dd, max_dd_pct, dd_start, dd_end


def create_summary_statistics(results: List[BacktestResult]) -> pd.DataFrame:
    """Create summary statistics from all results"""
    summary_stats = []

    for result in results:
        if result.success and not result.trades_df.empty:
            try:
                stats_df = compute_strategy_statistics(
                    result.trades_df,
                    result.stats_df,
                    result.strategy_name,
                    result.timeframe
                )
                if not stats_df.empty:
                    summary_stats.append(stats_df)
            except Exception as e:
                logger.warning(f"Failed to compute stats for {result.strategy_name}: {e}")

    if summary_stats:
        summary_df = pd.concat(summary_stats, ignore_index=True)
        logger.info(f"Created summary with {len(summary_df)} strategies")
        return summary_df
    else:
        logger.warning("No statistics to summarize")
        return pd.DataFrame()


