# src/brazillian_e_commerce/mart/mart_runner.py

from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.mart.builders import BUILDERS


def run_mart(br_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["mart"]

    if br_name:
        _run_single(spark, br_name, config[br_name])
    else:
        for name, cfg in config.items():
            _run_single(spark, name, cfg)


def _run_single(spark, name, cfg):
    tables = {k: spark.read.table(v) for k, v in cfg["source_tables"].items()}
    df = BUILDERS[name](**tables)
    write_table(df, cfg["target_table"], mode="overwrite")
