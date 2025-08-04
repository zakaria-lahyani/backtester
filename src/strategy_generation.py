from typing import Dict, List, Any
import logging
from jinja2 import Environment, FileSystemLoader
from itertools import product

from src.data_structure import IndicatorConfig, StrategyTemplate

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 3. STRATEGY GENERATION FUNCTIONS
# ============================================================================

def generate_simple_strategy_contexts(indicator_config: IndicatorConfig) -> List[Dict[str, Any]]:
    """Generate all parameter combinations for strategies"""
    contexts = []

    # Create all combinations of periods and timeframes
    for period, timeframe in product(indicator_config.periods, indicator_config.timeframes):
        context = {
            'period': period,
            'timeframe': timeframe,
            'signal_name': indicator_config.name
        }

        # Add additional parameters
        context.update(indicator_config.additional_params)
        contexts.append(context)

    logger.info(f"Generated {len(contexts)} strategy contexts")
    return contexts

def generate_combined_strategy_contexts(indicator_config: IndicatorConfig) -> List[Dict[str, Any]]:
    """
    Generate all parameter combinations for multi-timeframe strategies.
    Ensures that higher_timeframe > lower_timeframe.
    """
    contexts = []

    # Convert timeframes to integers for proper comparison
    sorted_timeframes = sorted(indicator_config.timeframes, key=lambda x: int(x))

    # Create all combinations of HTF and LTF with ordering constraint
    for period_htf, higher_timeframe, period_ltf, lower_timeframe in product(
        indicator_config.periods,
        sorted_timeframes,
        indicator_config.periods,
        sorted_timeframes
    ):
        if int(higher_timeframe) <= int(lower_timeframe):
            continue  # Ensure higher_timeframe is strictly greater

        context = {
            "period_htf": period_htf,
            "higher_timeframe": higher_timeframe,
            "period_ltf": period_ltf,
            "lower_timeframe": lower_timeframe,
            "signal_name": indicator_config.name
        }

        # Add any extra parameters from config
        context.update(indicator_config.additional_params)
        contexts.append(context)

    logger.info(f"Generated {len(contexts)} combined (multi-timeframe) strategy contexts")
    return contexts


def render_strategy_from_template(template: StrategyTemplate, context: Dict[str, Any], template_path: str) -> str:
    """Render a strategy YAML from template and context"""
    try:
        # Setup Jinja environment
        env = Environment(loader=FileSystemLoader(template_path))
        jinja_template = env.get_template(template.name)

        # Render the template
        rendered_yaml = jinja_template.render(context)
        return rendered_yaml

    except Exception as e:
        logger.error(f"Failed to render template {template.name}: {e}")
        raise


def generate_all_strategies(
        templates: List[StrategyTemplate],
        contexts: List[Dict[str, Any]],
        template_path: str
) -> List[str]:
    """Generate all strategy YAML strings"""
    strategies = []

    for template in templates:
        for context in contexts:
            try:
                strategy_yaml = render_strategy_from_template(template, context, template_path)
                strategies.append(strategy_yaml)
            except Exception as e:
                logger.warning(f"Failed to generate strategy from {template.name}: {e}")

    logger.info(f"Generated {len(strategies)} strategy configurations")
    return strategies

