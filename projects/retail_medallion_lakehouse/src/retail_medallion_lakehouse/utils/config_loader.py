import yaml
import importlib.resources as pkg_resources
from retail_medallion_lakehouse import config


def load_config(dataset_name: str):

    with pkg_resources.files(config).joinpath("settings.yaml").open("r") as file:
        configs = yaml.safe_load(file)

    return configs[dataset_name]
