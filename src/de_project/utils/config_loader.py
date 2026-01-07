import yaml
import os

def load_config(config_path: str = None) -> dict:
    """
    Load YAML configuration.
    
    Args:
        config_path (str, optional): Path to YAML config file. 
                                     Defaults to '../config/app_config.yaml'.
    
    Returns:
        dict: Loaded YAML config.
    
    Raises:
        FileNotFoundError: If the YAML file does not exist.
        yaml.YAMLError: If the YAML is invalid.
    """
    if not config_path:
        # Resolve path relative to this file
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../config/app_config.yaml"
        )

    config_path = os.path.normpath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file: {e}")

    return config
