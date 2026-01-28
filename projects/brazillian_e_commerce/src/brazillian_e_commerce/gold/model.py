from brazillian_e_commerce.utils.fact_builder import build_fact
from brazillian_e_commerce.utils.dim_builder import build_dim
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark


def run_model(table_name: str | None = None):
    spark = get_spark()
    config = load_config("tables.yaml")["gold"]

    tables = {table_name: config[table_name]} if table_name else config

    for name, cfg in tables.items():
        sources = {
            k: spark.read.table(v)
            for k, v in cfg["source_tables"].items()
        }

        df = (
            build_fact(name, **sources)
            if cfg["type"] == "fact"
            else build_dim(name, **sources)
        )

        write_table(
            df=df,
            target_table=cfg["target_table"],
            mode="overwrite",
            overwrite_schema=True
        )
