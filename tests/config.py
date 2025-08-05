from src.backtest import  parse_strategy_yaml, build_trading_signals, execute_backtest, \
    process_trade_results
from src.configuration import read_yaml_config, extract_indicator_config, create_backtest_config
from src.data_structure import BacktestResult
from src.file_identifier import build_file_column_reference, identify_required_columns, find_files_for_strategy, \
    remove_matching_suffix
from src.loader import load_strategy_data
from src.strategy_generation import generate_combined_strategy_contexts, generate_all_strategies, \
    generate_simple_strategy_contexts
from src.template_parser import load_all_strategy_templates
from src.timeframe_merge import merge_timeframes
import pandas as pd

indicator = "keltner"
symbol = "xauusd"
strategy_type: str = "simple"
# strategy_type: str = "combined"

if strategy_type == "combined":
    config_path: str = "../config/backtester_combined.yaml"
else:
    config_path: str = "../config/backtester_default.yaml"

config_data = read_yaml_config(config_path)
indicator_config = extract_indicator_config(config_data, indicator)
backtest_config = create_backtest_config(symbol, indicator, strategy_type, config_data)
templates = load_all_strategy_templates(backtest_config.template_path, indicator_config.templates)

# ============================================================================
# strategies
# ============================================================================

if strategy_type == "combined":
    contexts = generate_combined_strategy_contexts(indicator_config)
else:
    contexts = generate_simple_strategy_contexts(indicator_config)


strategies = generate_all_strategies(templates, contexts, backtest_config.template_path)

strategy_yaml = strategies[-1]

# print(contexts)

file_reference = build_file_column_reference(backtest_config.symbol, backtest_config.data_path, backtest_config.timeframe_names)
print(file_reference)


# ============================================================================
# run_single_backtest(strategy, file_reference, backtest_config)
# ============================================================================
# result = run_single_backtest(strategy, file_reference, backtest_config)
# strategy = parse_strategy_yaml(strategy_yaml)
# strategy_name = strategy.get("name", "unknown_strategy")
# timeframes = strategy.get("timeframes")
#
# required_columns = identify_required_columns(strategy_yaml)
# print(f"required_columns: {required_columns}")
#
# # if strategy_type == "combined":
# #     required_columns = remove_matching_suffix(required_columns)
# #
# # print(required_columns)
# #
# files_needed: dict[str, dict[str, list[str]]] = find_files_for_strategy(required_columns, file_reference)
# from collections import defaultdict
#
# files_needed = defaultdict(lambda: defaultdict(set))
#
# for column, timeframe in required_columns:
#     print(f"{column} - {timeframe}")
#     if timeframe and timeframe in file_reference:
#         print(f"{timeframe} exist")
#         for file_path, columns in file_reference[timeframe].items():
#             print(f"{file_path}")
#             if column in columns:
#                 files_needed[timeframe][file_path].add(column)
#
# # Convert sets to lists
# result = {
#     tf: {fp: sorted(list(cols)) for fp, cols in file_dict.items()}
#     for tf, file_dict in files_needed.items()
# }
#
# print(result)
#
# dataframes = load_strategy_data(files_needed, timeframes)


# timeframe_data = merge_timeframes(dataframes)
#
# main_timeframe = "_".join(timeframes)
# if main_timeframe not in timeframe_data:
#     raise ValueError(f"No data available for timeframe: {main_timeframe}")
#
# data = timeframe_data[main_timeframe]
# data["time"] = pd.to_datetime(data["time"])
# data = data.set_index("time")
#
# # 5. Build signals
# entries, exits = build_trading_signals(strategy, data)
#
# # 6. Execute backtest
# stats_df, trades_df = execute_backtest(entries, exits, data, backtest_config)
#
# print(entries)
# print(stats_df)
#
# # 7. Process results
# trades_df = process_trade_results(trades_df)
#
# br= BacktestResult(
#     strategy_name=strategy_name,
#     timeframe=main_timeframe,
#     trades_df=trades_df,
#     stats_df=stats_df,
#     success=True
# )
#
#
