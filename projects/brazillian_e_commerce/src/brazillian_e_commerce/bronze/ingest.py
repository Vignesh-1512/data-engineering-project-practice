from brazillian_e_commerce.utils.file_read import read_table
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.metadata import add_ingestion_metadata
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark


def run_ingest(table_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["bronze"]

    tables = {table_name: config[table_name]} if table_name else config

    for _, cfg in tables.items():
        df = read_table(
            spark=spark,
            source_table=cfg["source_table"],
            load_type=cfg["load_type"],
            watermark_col=cfg.get("watermark_column"),
            target_table=cfg["target_table"]
        )

        df = add_ingestion_metadata(df, "olist_kaggle")

        write_table(
            df=df,
            target_table=cfg["target_table"],
            mode="overwrite" if cfg["load_type"] == "full" else "append"
        )
