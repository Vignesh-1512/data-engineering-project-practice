from brazillian_e_commerce.utils.data_cast import cast_columns
from brazillian_e_commerce.utils.data_prep import clean_data
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark


def run_refine(table_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["silver"]

    tables = {table_name: config[table_name]} if table_name else config

    for _, cfg in tables.items():
        df = spark.read.table(cfg["source_table"])

        df = cast_columns(df, cfg.get("casts", {}))
        df = clean_data(df, cfg.get("clean_rules", {}))

        write_table(
            df=df,
            target_table=cfg["target_table"],
            mode="overwrite",
            overwrite_schema=True
        )
