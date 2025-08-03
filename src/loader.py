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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 5. DATA LOADING FUNCTIONS
# ============================================================================

def load_required_columns_from_file(file_path: str, required_columns: List[str]) -> pd.DataFrame:
    """Load only required columns from a parquet file"""
    base_cols = ["time", "open", "high", "low", "close"]
    columns_to_load = base_cols + required_columns

    try:
        df = pd.read_parquet(file_path, columns=columns_to_load)
        logger.debug(f"Loaded {len(columns_to_load)} columns from {os.path.basename(file_path)}")
        return df
    except Exception as e:
        logger.error(f"Failed to load data from {file_path}: {e}")
        raise


def merge_timeframe_data(files_data: List[pd.DataFrame]) -> pd.DataFrame:
    """Merge data from multiple files for the same timeframe"""
    if not files_data:
        return pd.DataFrame()

    if len(files_data) == 1:
        return files_data[0]

    # Merge all dataframes
    merged_df = pd.concat(files_data, axis=1)

    # Remove duplicate columns
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    return merged_df


def load_strategy_data(
        files_needed: Dict[str, Dict[str, List[str]]],
        strategy_timeframes: List[str]
) -> Dict[str, pd.DataFrame]:
    """Load all required data for a strategy"""
    timeframe_data = {}

    for tf in strategy_timeframes:
        if tf in files_needed:
            files_data = []

            for file_path, columns in files_needed[tf].items():
                df = load_required_columns_from_file(file_path, columns)
                files_data.append(df)

            if files_data:
                merged_df = merge_timeframe_data(files_data)
                timeframe_data[tf] = merged_df

    return timeframe_data

