from brazillian_e_commerce.utils.spark import get_spark
from brazillian_e_commerce.utils.write import write_table
from brazillian_e_commerce.utils.config import load_config

from brazillian_e_commerce.silver.transform_orders import transform_orders
from brazillian_e_commerce.silver.transform_order_items import transform_order_items
from brazillian_e_commerce.silver.transform_products import transform_products
from brazillian_e_commerce.silver.transform_customers import transform_customers


TRANSFORM_MAP = {
    "orders": transform_orders,
    "order_items": transform_order_items,
    "products": transform_products,
    "customers": transform_customers,
}


def run_silver(table_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["silver"]

    if table_name:
        cfg = config[table_name]
        df = spark.read.table(cfg["source_table"])
        result = TRANSFORM_MAP[table_name](df)
        write_table(result, cfg["target_table"])

    else:
        for table, cfg in config.items():
            df = spark.read.table(cfg["source_table"])
            result = TRANSFORM_MAP[table](df)
            write_table(result, cfg["target_table"])
