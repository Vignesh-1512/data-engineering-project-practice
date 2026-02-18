import yaml
import importlib.resources as pkg_resources
from retail_medallion_lakehouse import config


def load_config():
    """
    Load full settings.yaml packaged inside the wheel.
    """
    with pkg_resources.files(config).joinpath("settings.yaml").open("r") as file:
        return yaml.safe_load(file)
