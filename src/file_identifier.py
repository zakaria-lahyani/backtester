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
# 4. FILE IDENTIFICATION FUNCTIONS
# ============================================================================

def build_file_column_reference(symbol: str, data_path: str) -> Dict[str, Dict[str, List[str]]]:
    """Build reference of which columns are in which files"""
    timeframes = {
        "1": "M1", "5": "M5", "15": "M15",
        "30": "M30", "60": "H1", "240": "H4"
    }

    column_reference = {tf: {} for tf in timeframes.keys()}

    if not os.path.exists(data_path):
        logger.warning(f"Data path does not exist: {data_path}")
        return column_reference

    for folder in os.listdir(data_path):
        folder_path = os.path.join(data_path, folder)
        if not os.path.isdir(folder_path):
            continue

        for file in os.listdir(folder_path):
            for timeframe in timeframes.keys():
                if file.startswith(f"{symbol}_{timeframe}_") and file.endswith('.parquet'):
                    file_path = os.path.join(folder_path, file)

                    try:
                        # Read parquet metadata only
                        parquet_file = pq.ParquetFile(file_path)
                        columns = parquet_file.schema.names
                        column_reference[timeframe][file_path] = columns
                    except Exception as e:
                        logger.warning(f"Could not read metadata from {file_path}: {e}")

                    break

    total_files = sum(len(files) for files in column_reference.values())
    logger.info(f"Built file reference for {total_files} files")
    return column_reference


def identify_required_columns(strategy_yaml: str) -> List[Tuple[str, Optional[str]]]:
    """Identify which columns are required by a strategy"""
    try:
        strategy = yaml.safe_load(strategy_yaml)
        required = set()

        def scan_conditions(conditions):
            for cond in conditions:
                signal_col = cond.get("signal")
                value_ref = cond.get("value")
                tf = cond.get("timeframe")

                if isinstance(signal_col, str):
                    required.add((signal_col, tf))

                if isinstance(value_ref, str):
                    required.add((value_ref, tf))

        # Scan entry and exit conditions
        for side in ("long", "short"):
            if "entry" in strategy and side in strategy["entry"]:
                scan_conditions(strategy["entry"][side]["conditions"])

            if "exit" in strategy and side in strategy["exit"]:
                scan_conditions(strategy["exit"][side]["conditions"])

        return sorted(list(required))

    except Exception as e:
        logger.error(f"Failed to identify required columns: {e}")
        return []


def find_files_for_strategy(
        required_columns: List[Tuple[str, Optional[str]]],
        file_reference: Dict[str, Dict[str, List[str]]]
) -> Dict[str, Dict[str, List[str]]]:
    """Find which files contain the required columns"""
    from collections import defaultdict

    files_needed = defaultdict(lambda: defaultdict(set))

    for column, timeframe in required_columns:
        if timeframe and timeframe in file_reference:
            for file_path, columns in file_reference[timeframe].items():
                if column in columns:
                    files_needed[timeframe][file_path].add(column)

    # Convert sets to lists
    result = {
        tf: {fp: sorted(list(cols)) for fp, cols in file_dict.items()}
        for tf, file_dict in files_needed.items()
    }

    return result

