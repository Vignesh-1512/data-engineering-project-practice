from brazillian_e_commerce.utils.spark import get_spark
from brazillian_e_commerce.utils.write import write_table
from brazillian_e_commerce.utils.config import load_config

from brazillian_e_commerce.final.transform_sales_performance import (
    build_sales_performance
)


def run_final(br_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["final"]

    if br_name:
        _run_single_final(spark, config[br_name])
    else:
        for cfg in config.values():
            _run_single_final(spark, cfg)


def _run_single_final(spark, cfg):
    sources = cfg["source_tables"]

    fact_sales = spark.read.table(sources["fact_sales"])
    fact_orders = spark.read.table(sources["fact_orders"])
    dim_products = spark.read.table(sources["dim_products"])
    dim_date = spark.read.table(sources["dim_date"])

    df = build_sales_performance(
        fact_sales,
        fact_orders,
        dim_products,
        dim_date
    )

    write_table(
        df=df,
        target_table=cfg["target_table"],
        mode="overwrite"
    )
