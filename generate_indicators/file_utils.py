import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def create_folder(path: str):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {str(e)}")
            raise

def save_dataframe_to_parquet(df: pd.DataFrame, path: str, engine: str = "pyarrow"):
    try:
        create_folder(path)
        df.to_parquet(path, engine=engine, index=False)
        logger.info(f"Saved DataFrame to {path}")
    except Exception as e:
        logger.error(f"Failed to save DataFrame to {path}: {str(e)}")
        raise
