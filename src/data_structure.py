from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass(frozen=True)
class BacktestConfig:
    """Configuration for backtesting"""
    symbol: str
    indicator: str
    strategy_type: str
    data_path: str
    save_path: str
    template_path: str
    initial_capital: float
    point_value: float
    timeframe_names: Dict[str, str]
    frequency_map: Dict[str, str]


@dataclass(frozen=True)
class IndicatorConfig:
    """Indicator configuration from YAML"""
    name: str
    periods: List[str]
    timeframes: List[str]
    templates: List[str]
    additional_params: Dict[str, Any]


@dataclass(frozen=True)
class StrategyTemplate:
    """Strategy template information"""
    name: str
    content: str
    parameters: Dict[str, Any]


@dataclass(frozen=True)
class BacktestResult:
    """Result of a single backtest"""
    strategy_name: str
    timeframe: str
    trades_df: pd.DataFrame
    stats_df: pd.DataFrame
    success: bool
    error_message: Optional[str] = None

