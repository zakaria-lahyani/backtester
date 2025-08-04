import logging
from indicators.indicator_processor import IndicatorProcessor
from .data_loader import load_yaml_config
from .file_utils import save_dataframe_to_parquet
from .config import TIMEFRAMES

logger = logging.getLogger(__name__)

def generate_indicator_data_to_optimise(base_indicator_conf_path: str, indicator: str, data: dict[str, object], symbol: str, path_save: str):
    logger.info(f"Computing indicator: {indicator}")

    indicator_configs = {
        tf: load_yaml_config(f"{base_indicator_conf_path}/{indicator}.yaml")
        for tf in TIMEFRAMES
    }

    indicators = IndicatorProcessor(configs=indicator_configs, historicals=data, is_bulk=True)

    for tf, _ in data.items():
        logger.info(f"Processing timeframe: {tf}")
        try:
            df_result = indicators.get_historical_indicator_data(tf)
            path = f"{path_save}/{indicator}/{symbol}_{tf}_{indicator}.parquet"
            save_dataframe_to_parquet(df_result, path)
        except Exception as e:
            logger.error(f"Failed to generate indicator data for {indicator} timeframe {tf}: {str(e)}")
            raise
