from brazillian_e_commerce.utils.file_read import read_table
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.metadata import add_ingestion_metadata
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark
from brazillian_e_commerce.utils.exceptions import (
    ConfigError, DataReadError, DataWriteError
)
from brazillian_e_commerce.utils.path_builder import build_fqn


# optional generator import
from brazillian_e_commerce.utils.incremental_generator import input_data_generator


def run_ingest(
        layer: str,
        table_name: str | None = None,
        load_type: str | None = None,
        mode: str | None = None
):

    """
    Bronze ingestion.

    - full -> read everything
    - incremental -> watermark filter OR generator
    - append only
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

            cfg_load_type = cfg["load_type"]

            if cfg_load_type == "full":
                final_load_type = "full"

            elif cfg_load_type == "incremental":
                if not spark.catalog.tableExists(target_fqn):
                    final_load_type = "full"   # first time bootstrap
                else:
                    final_load_type = "incremental"

            print(f"\n→ Ingesting table : {name}")
            print(f"  Source table    : {source_fqn}")
            print(f"  Target table    : {target_fqn}")
            print(f"  Load type       : {final_load_type}")

            # ---------- READ ----------
            if final_load_type == "incremental" and cfg.get("watermark_column"):
                df = read_table(
                    spark=spark,
                    source_table=source_fqn,
                    load_type="incremental",
                    watermark_col=cfg.get("watermark_column"),
                    target_table=target_fqn
                )
            else:
                df = read_table(
                    spark=spark,
                    source_table=source_fqn,
                    load_type="full"
                )

            # ---------- OPTIONAL GENERATOR ----------
            # if you want synthetic incremental rows
            if df.isEmpty():
                print("  No new source rows → using generator")
                schema = spark.read.table(source_fqn).schema
                df = input_data_generator(spark, name, schema)


            print(f"  Rows read       : {df.count()}")

        except Exception as e:
            raise DataReadError(
                f"[Bronze] Failed reading '{source_fqn}' "
                f"for table '{name}'. Reason: {str(e)}"
            )

        try:
            df = add_ingestion_metadata(df, "olist_kaggle")

            write_mode = "overwrite" if final_load_type == "full" else "append"
            
            write_table(
                df=df,
                target_table=target_fqn,
                mode=config.get("write_mode","append")
            )

            print(f"  Bronze load completed for {name}")

        except Exception as e:
            raise DataWriteError(
                f"[Bronze] Failed writing '{target_fqn}'. Reason: {str(e)}"
            )
