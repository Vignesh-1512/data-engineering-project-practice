from pyspark.sql.window import Window
from pyspark.sql import functions as F


from brazillian_e_commerce.utils.data_cast import cast_columns
from brazillian_e_commerce.utils.data_prep import clean_data
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark
from brazillian_e_commerce.utils.exceptions import ConfigError, DataWriteError
from brazillian_e_commerce.utils.path_builder import build_fqn
from brazillian_e_commerce.utils.merge_table import merge_upsert

def run_refine(
        layer: str, 
        table_name: str | None = None,
        mode: str ="overwrite"
):


    """
    Runs the refinement (Silver) pipeline.

    Reads data from bronze schema, applies casting, renaming,
    and data quality rules, then writes to silver schema.

    Args:
        layer (str): Pipeline layer name (silver)
        table_name (str | None): Specific table or all tables
        mode (str): Write mode for target table

    Raises:
        ConfigError: If silver configuration fails
        TransformationError: If casting or cleaning fails
        DataWriteError: If write to silver fails
    """

    try:
        spark = get_spark()
        config = load_config("tables.yaml")[layer]

        print("\n[SILVER REFINEMENT STARTED]")

    except Exception as e:
        raise ConfigError(f"Failed to load {layer} config. Reason: {str(e)}")

    tables = {table_name: config[table_name]} if table_name else config

    for name, cfg in tables.items():
        source_fqn = build_fqn(
            cfg["catalog"],
            cfg["source_schema"],
            cfg["source_table"]
        )

        target_fqn = build_fqn(
            cfg["catalog"],
            cfg["target_schema"],
            cfg["table_name"]
        )

        print(f"\n→ Refining table : {name}")
        print(f"  Source table   : {source_fqn}")
        print(f"  Target table   : {target_fqn}")

        df = spark.read.table(source_fqn)
        print(f"  Rows before    : {df.count()}")

        print(f"  Applying casts : {list(cfg['casts'].keys())}")
        df = cast_columns(df, cfg.get("casts", {}))

        print(f"  Applying clean rules")
        df = clean_data(df, cfg.get("clean_rules", {}))

        print(f"  Rows after     : {df.count()}")

        # ----------------------
        # Dynamic dedupe
        # ----------------------
        merge_key = cfg.get("merge_key")
        order_col = cfg.get("dedupe_order_by", "ingestion_ts")

        window = Window.partitionBy(merge_key) \
                    .orderBy(F.col(order_col).desc())

        df = (
            df.withColumn("rn", F.row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
        )

        print(f"  Rows after dedupe : {df.count()}")

        try:
            merge_upsert(
                spark=spark,
                df=df,
                target_table=target_fqn,
                merge_key=merge_key
            )
            print(f"  ✅ Silver merge completed for {name}")

        except Exception as e:
            raise DataWriteError(
                f"[Silver] Failed writing '{target_fqn}'. Reason: {str(e)}"
            )
