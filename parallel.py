import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
import pandas as pd
from typing import List, Tuple

from src.backtest import run_backtest
from src.configuration import read_yaml_config, extract_indicator_config, create_backtest_config
from src.file_identifier import build_file_column_reference
from src.results import save_all_results, save_summary_statistics, append_parquet_files
from src.statistics import create_summary_statistics
from src.strategy_generation import (
    generate_all_strategies,
    generate_simple_strategy_contexts,
    generate_combined_strategy_contexts
)
from src.template_parser import load_all_strategy_templates

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def prepare_backtest_tasks(symbol: str, indicator: str, strategy_type: str, config_path: str):
    logger.info(f"[{indicator}] Preparing backtest tasks...")

    config_data = read_yaml_config(config_path)
    indicator_config = extract_indicator_config(config_data, indicator)
    backtest_config = create_backtest_config(symbol, indicator, strategy_type, config_data)

    templates = load_all_strategy_templates(backtest_config.template_path, indicator_config.templates)

    contexts = (
        generate_combined_strategy_contexts(indicator_config)
        if strategy_type == "combined"
        else generate_simple_strategy_contexts(indicator_config)
    )

    strategies = generate_all_strategies(templates, contexts, backtest_config.template_path)

    file_reference = build_file_column_reference(
        backtest_config.symbol,
        backtest_config.data_path,
        backtest_config.timeframe_names
    )

    tasks = [
        (strategy_type, strategy_yaml, file_reference, backtest_config)
        for strategy_yaml in strategies
    ]
    return tasks, {"indicator": indicator, "backtest_config": backtest_config}


def run_all_indicators_global_streaming(symbol: str, indicators: List[str], strategy_type: str, config_path: str):
    logger.info("=== Preparing tasks for all indicators ===")
    all_tasks = []
    indicator_meta = {}

    for ind in indicators:
        tasks, meta = prepare_backtest_tasks(symbol, ind, strategy_type, config_path)
        all_tasks.extend([(ind, *t) for t in tasks])
        indicator_meta[ind] = meta

    logger.info(f"Prepared {len(all_tasks)} total backtests across {len(indicators)} indicators.")

    logger.info("=== Running all backtests in parallel (streaming save) ===")
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        future_to_task = {
            executor.submit(run_backtest, strategy_type, strategy_yaml, file_reference, backtest_config): (
                indicator, idx)
            for idx, (indicator, strategy_type, strategy_yaml, file_reference, backtest_config) in
            enumerate(all_tasks, 1)
        }

        for i, future in enumerate(as_completed(future_to_task), 1):
            indicator, idx = future_to_task[future]
            try:
                summary = future.result()  # now a dict
                logger.info(f"[{indicator}] Completed {summary['strategy_name']} ({idx}/{len(all_tasks)})")
            except Exception as e:
                logger.error(f"[{indicator}] Task {idx} failed: {e}")

    logger.info("=== Generate Summary ===")
    append_parquet_files(indicator_meta[indicator]["backtest_config"])

if __name__ == "__main__":
    symbol = "xauusd"
    indicators = [
        # "ichimoku", "sar", "rsi", "ursi", "aroon", "adx", "macd",
        # "stochrsi",
        #  "bb",
        "keltner",
        # "supertrend",
    ]
    # config_path = "config/backtester_combined.yaml"
    # strategy_type = "combined"
    config_path: str = "config/backtester_default.yaml"
    strategy_type: str = "simple"

    for indicator in indicators:
        run_all_indicators_global_streaming(symbol, [indicator], strategy_type, config_path)
