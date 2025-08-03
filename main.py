from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
import logging
import os
import yaml
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from itertools import product
import pyarrow.parquet as pq

from src.backtest import run_single_backtest
from src.configuration import read_yaml_config, extract_indicator_config, create_backtest_config
from src.file_identifier import build_file_column_reference
from src.results import save_all_results, save_summary_statistics
from src.statistics import create_summary_statistics
from src.strategy_generation import generate_strategy_contexts, generate_all_strategies
from src.template_parser import load_all_strategy_templates

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_functional_backtest_pipeline(
        symbol: str,
        indicator: str,
        strategy_type: str = "simple",
        config_path: str = "config/backtester_default.yaml"
) -> bool:
    """Main functional pipeline - clear step-by-step execution"""

    logger.info(f"=== Starting Functional Backtest Pipeline ===")
    logger.info(f"Symbol: {symbol}, Indicator: {indicator}, Type: {strategy_type}")

    try:
        # Step 1: Read configuration
        logger.info("Step 1: Reading configuration...")
        config_data = read_yaml_config(config_path)
        indicator_config = extract_indicator_config(config_data, indicator)
        backtest_config = create_backtest_config(symbol, indicator, strategy_type, config_data)

        # Step 2: Load strategy templates
        logger.info("Step 2: Loading strategy templates...")
        templates = load_all_strategy_templates(backtest_config.template_path, indicator_config.templates)

        # Step 3: Generate strategy contexts
        logger.info("Step 3: Generating strategy contexts...")
        contexts = generate_strategy_contexts(indicator_config)

        # Step 4: Generate all strategies
        logger.info("Step 4: Generating strategies...")
        strategies = generate_all_strategies(templates, contexts, backtest_config.template_path)
        logger.info(f"strategies : {len(strategies)}")

        # Step 5: Build file reference
        logger.info("Step 5: Building file reference...")
        file_reference = build_file_column_reference(backtest_config.symbol, backtest_config.data_path)

        # Step 6: Run all backtests
        logger.info(f"Step 6: Running {len(strategies)} backtests...")
        results = []
        for i, strategy_yaml in enumerate(strategies, 1):
            logger.info(f"Running backtest {i}/{len(strategies)}")
            result = run_single_backtest(strategy_yaml, file_reference, backtest_config)
            results.append(result)

        # Step 7: Create summary statistics
        logger.info("Step 7: Creating summary statistics...")
        summary_df = create_summary_statistics(results)

        # Step 8: Save all results
        logger.info("Step 8: Saving results...")
        save_all_results(results, backtest_config)
        save_summary_statistics(summary_df, backtest_config)

        # Final summary
        successful = len([r for r in results if r.success])
        logger.info(f"=== Pipeline Completed Successfully ===")
        logger.info(f"Total strategies: {len(results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {len(results) - successful}")

        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


def main():
    """Simple main function"""
    # Configuration
    symbol = "xauusd"
    indicator = "macd"
    strategy_type = "simple"
    success = run_functional_backtest_pipeline(symbol, indicator, strategy_type)

    print(success)

if __name__ == "__main__":
    main()