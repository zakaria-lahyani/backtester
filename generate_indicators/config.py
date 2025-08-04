import os

TIMEFRAMES = {
    "1": "M1",
    "5": "M5",
    "15": "M15",
    "30": "M30",
    "60": "H1",
    "240": "H4",
    "daily": "daily",
    "weekly": "weekly",
    "monthly": "monthly"
}

# Base paths can be overridden by environment variables for flexibility
BASE_INDICATOR_CONF_PATH = os.getenv(
    "BASE_INDICATOR_CONF_PATH",
    r"C:\Users\zak\Desktop\workspace\git\client\config\optimiser\indicators\parameters"
)

BASE_READER_PATH = os.getenv(
    "BASE_READER_PATH",
    r"C:\Users\zak\Desktop\workspace\datalake\raw"
)

BASE_SAVER_PATH = os.getenv(
    "BASE_SAVER_PATH",
    r"C:\Users\zak\Desktop\workspace\datalake\gold"
)

# Default symbol to process
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "xauusd")

# List of indicators to process
INDICATORS_LIST = [
    # "adx", "aroon", "atr", "bb", "ema", "obv", "rsi", "sar", "sma", "supertrend", "ursi",
    # "ichimoku_12_20", "ichimoku_12_26", "ichimoku_12_30", "ichimoku_12_35",
    # "ichimoku_15_20", "ichimoku_15_26", "ichimoku_15_30", "ichimoku_15_35",
    # "ichimoku_7_20", "ichimoku_7_26", "ichimoku_7_30", "ichimoku_7_35",
    # "ichimoku_9_20", "ichimoku_9_26", "ichimoku_9_30", "ichimoku_9_35",
    # "keltner_10", "keltner_14", "keltner_20", "keltner_25", "keltner_30",
    # "keltner_35", "keltner_40", "keltner_5", "keltner_50",
    # "macd_10", "macd_12", "macd_14", "macd_16", "macd_8",
    # "stochrsi_10_10", "stochrsi_10_14", "stochrsi_10_21", "stochrsi_10_7",
    # "stochrsi_14_10", "stochrsi_14_14", "stochrsi_14_21", "stochrsi_14_7",
    # "stochrsi_21_10", "stochrsi_21_14", "stochrsi_21_21", "stochrsi_21_7",
    # "stochrsi_7_10", "stochrsi_7_14", "stochrsi_7_21", "stochrsi_7_7"
    "keltner_35", "ichimoku_12_26",
]
