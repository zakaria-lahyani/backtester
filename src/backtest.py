from typing import Dict, List, Tuple, Any
import pandas as pd
import logging
import yaml

from src.data_structure import BacktestConfig, BacktestResult
from src.file_identifier import identify_required_columns, find_files_for_strategy
from src.loader import load_strategy_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 6. BACKTEST EXECUTION FUNCTIONS
# ============================================================================

def parse_strategy_yaml(strategy_yaml: str) -> Dict[str, Any]:
    """Parse strategy YAML into dictionary"""
    try:
        strategy = yaml.safe_load(strategy_yaml)
        return strategy
    except Exception as e:
        logger.error(f"Failed to parse strategy YAML: {e}")
        raise


def build_trading_signals(strategy: Dict[str, Any], data: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Build entry and exit signals from strategy and data"""
    try:
        # Build entry conditions
        entry_config = strategy["entry"]["long"]
        entry_conditions = [build_condition(c, data) for c in entry_config["conditions"]]
        entries = combine_conditions(entry_conditions, entry_config["mode"])

        # Build exit conditions
        exit_config = strategy["exit"]["long"]
        exit_conditions = [build_condition(c, data) for c in exit_config["conditions"]]
        exits = combine_conditions(exit_conditions, exit_config["mode"])

        return entries, exits
    except Exception as e:
        logger.error(f"Failed to build trading signals: {e}")
        raise


def build_condition(cond: Dict[str, Any], df: pd.DataFrame) -> pd.Series:
    """Build a single condition from configuration"""
    signal_col = cond["signal"]
    value_ref = cond["value"]
    operator = cond["operator"]

    # Get the signal series
    if signal_col not in df.columns:
        raise KeyError(f"Signal '{signal_col}' not found in DataFrame columns")

    signal_series = df[signal_col]

    # Resolve value: could be a column name or literal value
    if isinstance(value_ref, str) and value_ref in df.columns:
        value_series = df[value_ref]
        is_column_reference = True
    else:
        # It's a literal value - convert to appropriate type
        try:
            if isinstance(value_ref, str):
                value_literal = float(value_ref)
            else:
                value_literal = value_ref
        except (ValueError, TypeError):
            value_literal = value_ref

        # Create a series with the literal value
        value_series = pd.Series(value_literal, index=df.index)
        is_column_reference = False

    # Apply operators
    if operator == "crosses_above":
        return crosses_above(signal_series, value_series, is_column_reference)
    elif operator == "crosses_below":
        return crosses_below(signal_series, value_series, is_column_reference)
    elif operator == "changes_to":
        return changes_to(signal_series, value_series)
    elif operator == "remains":
        return remains(signal_series, value_series)
    elif operator == "gt" or operator == ">":
        return signal_series > value_series
    elif operator == "gte" or operator == ">=":
        return signal_series >= value_series
    elif operator == "lt" or operator == "<":
        return signal_series < value_series
    elif operator == "lte" or operator == "<=":
        return signal_series <= value_series
    elif operator == "eq" or operator == "==":
        return signal_series == value_series
    elif operator == "ne" or operator == "!=":
        return signal_series != value_series
    else:
        raise ValueError(f"Unsupported operator: {operator}")


def crosses_above(signal_series: pd.Series, value_series: pd.Series, is_column_reference: bool) -> pd.Series:
    """Check if signal crosses above value"""
    prev_signal = signal_series.shift(1)

    if is_column_reference:
        prev_value = value_series.shift(1)
        return (prev_signal <= prev_value) & (signal_series > value_series)
    else:
        return (prev_signal <= value_series) & (signal_series > value_series)


def crosses_below(signal_series: pd.Series, value_series: pd.Series, is_column_reference: bool) -> pd.Series:
    """Check if signal crosses below value"""
    prev_signal = signal_series.shift(1)

    if is_column_reference:
        prev_value = value_series.shift(1)
        return (prev_signal >= prev_value) & (signal_series < value_series)
    else:
        return (prev_signal >= value_series) & (signal_series < value_series)


def changes_to(signal_series: pd.Series, value_series: pd.Series) -> pd.Series:
    """Check if signal changes to a specific value"""
    prev_signal = signal_series.shift(1)
    return (signal_series == value_series) & (prev_signal != value_series)


def remains(signal_series: pd.Series, value_series: pd.Series) -> pd.Series:
    """Check if signal remains at a specific value"""
    prev_signal = signal_series.shift(1)
    return (signal_series == value_series) & (prev_signal == value_series)


def combine_conditions(condition_results: List[pd.Series], mode: str) -> pd.Series:
    """Combine multiple condition results"""
    if not condition_results:
        raise ValueError("No conditions to combine")

    result = condition_results[0]
    for condition in condition_results[1:]:
        if mode == "all":
            result = result & condition
        elif mode == "any":
            result = result | condition
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    return result


def execute_backtest(
        entries: pd.Series,
        exits: pd.Series,
        data: pd.DataFrame,
        config: BacktestConfig
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Execute backtest using VectorBT"""
    from vectorbt import Portfolio

    try:
        # Create portfolio
        pf = Portfolio.from_signals(
            close=data["close"],
            entries=entries,
            exits=exits,
            size=config.point_value,
            direction="longonly",
            freq="1min",
            init_cash=config.initial_capital,
            fees=0.0,
            slippage=0.0
        )

        # Get results
        stats_df = pf.stats()
        trades_df = pf.trades.records_readable

        return stats_df, trades_df

    except Exception as e:
        logger.error(f"Failed to execute backtest: {e}")
        raise


def process_trade_results(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean trade results"""
    if trades_df.empty:
        return trades_df

    # Convert timestamps
    trades_df["Entry Timestamp"] = pd.to_datetime(trades_df["Entry Timestamp"])
    trades_df["Exit Timestamp"] = pd.to_datetime(trades_df["Exit Timestamp"])

    # Calculate duration in minutes
    trades_df["Duration"] = (
                                    trades_df["Exit Timestamp"] - trades_df["Entry Timestamp"]
                            ).dt.total_seconds() / 60

    # Rename columns to standard format
    column_map = {
        "Exit Trade Id": "exit_trade_id",
        "Column": "column",
        "Size": "size",
        "Entry Timestamp": "entry_timestamp",
        "Avg Entry Price": "entry_price",
        "Entry Fees": "entry_fees",
        "Exit Timestamp": "exit_timestamp",
        "Avg Exit Price": "exit_price",
        "Exit Fees": "exit_fees",
        "PnL": "pnl",
        "Return": "return",
        "Direction": "direction",
        "Status": "status",
        "Position Id": "position_id",
        "Duration": "duration"
    }

    trades_df = trades_df.rename(columns=column_map)
    return trades_df


def run_single_backtest(
        strategy_yaml: str,
        file_reference: Dict[str, Dict[str, List[str]]],
        config: BacktestConfig
) -> BacktestResult:
    """Run a complete backtest for a single strategy"""
    try:
        # 1. Parse strategy
        strategy = parse_strategy_yaml(strategy_yaml)
        strategy_name = strategy.get("name", "unknown_strategy")
        timeframes = strategy.get("timeframes", ["1"])

        # 2. Identify required data
        required_columns = identify_required_columns(strategy_yaml)
        files_needed = find_files_for_strategy(required_columns, file_reference)

        # 3. Load data
        timeframe_data = load_strategy_data(files_needed, timeframes)

        # 4. Get main timeframe data
        main_timeframe = "_".join(timeframes)
        if main_timeframe not in timeframe_data:
            raise ValueError(f"No data available for timeframe: {main_timeframe}")

        data = timeframe_data[main_timeframe]
        data["time"] = pd.to_datetime(data["time"])
        data = data.set_index("time")

        # 5. Build signals
        entries, exits = build_trading_signals(strategy, data)

        # 6. Execute backtest
        stats_df, trades_df = execute_backtest(entries, exits, data, config)

        # 7. Process results
        trades_df = process_trade_results(trades_df)

        return BacktestResult(
            strategy_name=strategy_name,
            timeframe=main_timeframe,
            trades_df=trades_df,
            stats_df=stats_df,
            success=True
        )

    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        return BacktestResult(
            strategy_name="failed_strategy",
            timeframe="unknown",
            trades_df=pd.DataFrame(),
            stats_df=pd.DataFrame(),
            success=False,
            error_message=str(e)
        )

