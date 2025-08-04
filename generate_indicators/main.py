import logging
import sys
from generate_indicators.config import TIMEFRAMES, BASE_INDICATOR_CONF_PATH, BASE_READER_PATH, BASE_SAVER_PATH, DEFAULT_SYMBOL, INDICATORS_LIST
from generate_indicators.data_loader import load_data_for_timeframes
from generate_indicators.indicator_generator import generate_indicator_data_to_optimise

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    symbol = DEFAULT_SYMBOL

    try:
        data = load_data_for_timeframes(BASE_READER_PATH + f"/{symbol}/brut", symbol, TIMEFRAMES)
    except Exception as e:
        logger.error(f"Failed to load data for symbol {symbol}: {str(e)}")
        return

    for indicator in INDICATORS_LIST:
        try:
            generate_indicator_data_to_optimise(BASE_INDICATOR_CONF_PATH, indicator=indicator, data=data, path_save=BASE_SAVER_PATH + f"/{symbol}/indicators", symbol=symbol)
        except Exception as e:
            logger.error(f"Failed to generate indicator data for {indicator}: {str(e)}")

if __name__ == "__main__":
    main()
