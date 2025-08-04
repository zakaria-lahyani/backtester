import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_yaml_config(path: str):
    import yaml
    if not os.path.exists(path):
        logger.error(f"YAML config file does not exist: {path}")
        raise FileNotFoundError(f"YAML config file does not exist: {path}")
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load YAML config from {path}: {str(e)}")
        raise

def load_data_for_timeframes(base_path: str, symbol: str, timeframes: dict) -> dict[str, pd.DataFrame]:
    data = {}
    for tf in timeframes:
        file_path = os.path.join(base_path, f"{symbol.upper()}_{tf}.parquet")
        if not os.path.exists(file_path):
            logger.warning(f"Data file for timeframe {tf} does not exist: {file_path}")
            continue
        try:
            data[tf] = pd.read_parquet(file_path)
            logger.info(f"Loaded data for timeframe {tf} from {file_path}")
        except Exception as e:
            logger.error(f"Failed to load data for timeframe {tf} from {file_path}: {str(e)}")
            raise
    if not data:
        logger.error("No data loaded for any timeframe.")
        raise ValueError("No data loaded for any timeframe.")
    return data
