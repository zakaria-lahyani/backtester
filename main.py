import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

from src.backtest import run_backtest
from src.configuration import read_yaml_config, extract_indicator_config, create_backtest_config
from src.file_identifier import build_file_column_reference
from src.results import save_all_results, save_summary_statistics
from src.statistics import create_summary_statistics
from src.strategy_generation import (
    generate_all_strategies,
    generate_simple_strategy_contexts,
    generate_combined_strategy_contexts
)
from src.template_parser import load_all_strategy_templates

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_functional_backtest_pipeline(symbol: str, indicator: str, strategy_type: str, config_path: str) -> bool:
    """Main functional pipeline - now supports parallel execution."""
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
        if strategy_type == "combined":
            contexts = generate_combined_strategy_contexts(indicator_config)
        else:
            contexts = generate_simple_strategy_contexts(indicator_config)

        # Step 4: Generate all strategies
        logger.info("Step 4: Generating strategies...")
        strategies = generate_all_strategies(templates, contexts, backtest_config.template_path)

        # Step 5: Build file reference
        logger.info("Step 5: Building file reference...")
        file_reference = build_file_column_reference(
            backtest_config.symbol,
            backtest_config.data_path,
            backtest_config.timeframe_names
        )

        # Step 6: Run all backtests in parallel
        logger.info(f"Step 6: Running {len(strategies)} backtests in parallel...")
        results = []
        with ProcessPoolExecutor() as executor:
            future_to_strategy = {
                executor.submit(run_backtest, strategy_type, strategy_yaml, file_reference, backtest_config): i
                for i, strategy_yaml in enumerate(strategies, 1)
            }

            for future in as_completed(future_to_strategy):
                i = future_to_strategy[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"Backtest {i}/{len(strategies)} completed")
                except Exception as e:
                    logger.error(f"Backtest {i} failed: {e}")

        # Step 7: Create summary statistics
        logger.info("Step 7: Creating summary statistics...")
        summary_df = create_summary_statistics(results)

        # Step 8: Save all results
        logger.info("Step 8: Saving results...")
        save_all_results(results, backtest_config)
        save_summary_statistics(strategy_type, summary_df, backtest_config)

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


def main(symbol: str, indicator: str, strategy_type: str, config_path: str):
    success = run_functional_backtest_pipeline(symbol, indicator, strategy_type, config_path)
    print(success)


if __name__ == "__main__":
    symbol = "xauusd"
    indicators = ["macd"]

    config_path = "config/backtester_combined.yaml"
    strategy_type = "combined"

    for indicator in indicators:
        print(f"Compute {strategy_type} indicator {indicator}")
        main(symbol, indicator, strategy_type, config_path)

#
# def main(symbol:str, indicator:str, strategy_type:str, config_path:str ):
#     """Simple main function"""
#     # Configuration
#     success = run_functional_backtest_pipeline(symbol, indicator, strategy_type, config_path)
#
#     print(success)
#
# if __name__ == "__main__":
#     symbol = "xauusd"
#     # indicators = ["ichimoku", "sar", "rsi", "ursi", "aroon", "adx", "macd", "stochrsi", "bb", "keltner", "supertrend", ]
#     # indicators = [ ]
#
#     indicators = ["macd"]
#
#     # config_path: str = "config/backtester_default.yaml"
#     # strategy_type: str = "simple"
#     # for indicator in indicators:
#     #     print(f"Compute {strategy_type} indicator {indicator}")
#     #     main(symbol, indicator, strategy_type, config_path)
#
#     config_path: str = "config/backtester_combined.yaml"
#     strategy_type: str = "combined"
#     for indicator in indicators:
#         print(f"Compute {strategy_type} indicator {indicator}")
#         main(symbol, indicator, strategy_type, config_path)
