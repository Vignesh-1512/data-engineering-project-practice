from brazillian_e_commerce.utils.file_read import read_table
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.metadata import add_ingestion_metadata
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark
from brazillian_e_commerce.utils.exceptions import (
    ConfigError, DataReadError, DataWriteError
)
from brazillian_e_commerce.utils.path_builder import build_fqn


def run_ingest(
        layer: str, 
        table_name: str | None = None,
        mode: str ="overwrite"
):

    """
    Runs the ingestion (Bronze) pipeline.

    Reads data from source schema, applies incremental or full load logic,
    adds ingestion metadata, and writes into the bronze schema.

    Args:
        layer (str): Pipeline layer name (bronze)
        table_name (str | None): Specific table to ingest or all tables
        mode (str): Write mode (overwrite / append)

    Raises:
        ConfigError: If configuration loading fails
        DataReadError: If source table read fails
        DataWriteError: If writing to bronze table fails
    """

    try:
        spark = get_spark()
        config = load_config("tables.yaml")[layer]
    except Exception as e:
        raise ConfigError(f"Failed to load {layer} config. Reason: {str(e)}")

    print("\n[BRONZE INGESTION STARTED]")

    tables = {table_name: config[table_name]} if table_name else config

    for name, cfg in tables.items():
        try:
            source_fqn = build_fqn(
                cfg["catalog"],
                cfg["source_schema"],
                cfg.get("source_table", name)
            )

            target_fqn = build_fqn(
                cfg["catalog"],
                cfg["target_schema"],
                cfg["table_name"]
            )

            print(f"\n→ Ingesting table : {name}")
            print(f"  Source table    : {source_fqn}")
            print(f"  Target table    : {target_fqn}")
            print(f"  Load type       : {cfg['load_type']}")
        
            df = read_table(
                spark=spark,
                source_table=source_fqn,
                load_type=cfg["load_type"],
                watermark_col=cfg.get("watermark_column"),
                target_table=target_fqn
            )

            print(f"  Rows read       : {df.count()}")

        except Exception as e:
            raise DataReadError(
                f"[Bronze] Failed reading '{source_fqn}' "
                f"for table '{name}'. Reason: {str(e)}"
            )

        try:
            df = add_ingestion_metadata(df, "olist_kaggle")

            write_table(
                df=df,
                target_table=target_fqn,
                mode=mode if cfg["load_type"] == "full" else "append"
            )

            print(f" Bronze load completed for {name}")
        except Exception as e:
            raise DataWriteError(
                f"[Bronze] Failed writing '{target_fqn}'. Reason: {str(e)}"
            )
