
from typing import List
import logging
import os

from src.data_structure import StrategyTemplate

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 2. STRATEGY TEMPLATE READING FUNCTIONS
# ============================================================================

def read_strategy_template(template_path: str, template_name: str) -> str:
    """Read a single strategy template file"""
    template_file = os.path.join(template_path, template_name)

    try:
        with open(template_file, 'r') as f:
            content = f.read()
        logger.debug(f"Read template: {template_name}")
        return content
    except Exception as e:
        logger.error(f"Failed to read template {template_name}: {e}")
        raise


def load_all_strategy_templates(template_path: str, template_names: List[str]) -> List[StrategyTemplate]:
    """Load all strategy templates"""
    templates = []

    for template_name in template_names:
        try:
            content = read_strategy_template(template_path, template_name)
            template = StrategyTemplate(
                name=template_name,
                content=content,
                parameters={}
            )
            templates.append(template)
        except Exception as e:
            logger.warning(f"Skipping template {template_name}: {e}")

    logger.info(f"Loaded {len(templates)} strategy templates")
    return templates

