from typing import Dict, Any
import logging
import os
import yaml

from src.data_structure import IndicatorConfig, BacktestConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 1. CONFIGURATION READING FUNCTIONS
# ============================================================================

def read_yaml_config(config_path: str) -> Dict[str, Any]:
    """Read YAML configuration file"""
    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        logger.info(f"Successfully read config from {config_path}")
        return config_data
    except Exception as e:
        logger.error(f"Failed to read config from {config_path}: {e}")
        raise


def extract_indicator_config(config_data: Dict[str, Any], indicator_name: str) -> IndicatorConfig:
    """Extract indicator configuration from config data"""
    indicators = config_data.get('indicators', {})

    if indicator_name not in indicators:
        raise ValueError(f"Indicator '{indicator_name}' not found in configuration")

    indicator_data = indicators[indicator_name]

    return IndicatorConfig(
        name=indicator_name,
        periods=indicator_data.get('periods', []),
        timeframes=indicator_data.get('timeframes', []),
        templates=indicator_data.get('templates', []),
        additional_params=indicator_data.get('additional_params', {})
    )


def create_backtest_config(
        symbol: str,
        indicator: str,
        strategy_type: str,
        config_data: Dict[str, Any]
) -> BacktestConfig:
    """Create backtest configuration from config data"""

    # Extract paths
    paths = config_data.get('paths', {})
    base_data_path = paths.get('base_data_path', '')
    base_save_path = paths.get('base_save_path', '')
    template_dir_path = paths.get('template_dir_path', '')

    # Extract backtest settings
    backtest = config_data.get('backtest', {})

    return BacktestConfig(
        symbol=symbol,
        indicator=indicator,
        strategy_type=strategy_type,
        data_path=os.path.join(base_data_path, symbol, "indicators"),
        save_path=os.path.join(base_save_path, symbol, "backtest"),
        template_path=os.path.join(template_dir_path, strategy_type, indicator),
        initial_capital=backtest.get('initial_capital', 100_000.0),
        point_value=backtest.get('point_value', 100.0),
        timeframe_names=backtest.get('timeframe_names', {}),
        frequency_map=backtest.get('frequency_map', {})
    )
