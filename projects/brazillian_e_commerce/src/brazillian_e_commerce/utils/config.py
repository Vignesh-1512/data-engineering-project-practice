import yaml
from importlib import resources


def load_config(config_name: str):
    """
    Load YAML config packaged inside the wheel.
    """
    with resources.open_text(
        "brazillian_e_commerce.config",
        config_name
    ) as f:
        return yaml.safe_load(f)
