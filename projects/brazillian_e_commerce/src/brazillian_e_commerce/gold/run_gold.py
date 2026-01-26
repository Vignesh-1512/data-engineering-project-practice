from brazillian_e_commerce.utils.spark import get_spark
from brazillian_e_commerce.utils.write import write_table
from brazillian_e_commerce.utils.config import load_config

from brazillian_e_commerce.gold.facts.fact_orders import build_fact_orders
from brazillian_e_commerce.gold.facts.fact_sales import build_fact_sales
from brazillian_e_commerce.gold.dims.dim_customers import build_dim_customers
from brazillian_e_commerce.gold.dims.dim_products import build_dim_products
from brazillian_e_commerce.gold.dims.dim_date import build_dim_date


BUILDERS = {
    "fact_orders": build_fact_orders,
    "fact_sales": build_fact_sales,
    "dim_customers": build_dim_customers,
    "dim_products": build_dim_products,
    "dim_date": build_dim_date,
}


def run_gold(table_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["gold"]

    if table_name:
        _run_single_gold(spark, table_name, config[table_name])
    else:
        for name, cfg in config.items():
            _run_single_gold(spark, name, cfg)


def _run_single_gold(spark, name, cfg):
    df = spark.read.table(cfg["source_table"])

    result_df = BUILDERS[name](df)

    write_table(
        df=result_df,
        target_table=cfg["target_table"],
        mode="overwrite"
    )
