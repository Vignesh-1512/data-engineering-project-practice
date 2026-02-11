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


# =====================================================
# Helpers
# =====================================================

def _normalize_keys(keys):
    """Always return list[str] or None"""
    if not keys:
        return None
    return [keys] if isinstance(keys, str) else keys


def _dedupe(df, keys, order_col):
    """Apply window-based dedupe"""
    window = (
        Window
        .partitionBy(*keys)
        .orderBy(F.col(order_col).desc())
    )

    return (
        df.withColumn("rn", F.row_number().over(window))
          .filter("rn = 1")
          .drop("rn")
    )


# =====================================================
# MAIN
# =====================================================

def run_refine(
        layer: str,
        table_name: str | None = None,
        mode: str = "overwrite"
):
    """
    Silver refinement layer.

    Strategy:
    --------
    If merge_key exists  -> incremental MERGE
    If merge_key missing -> full OVERWRITE
    """

    # -------------------------------------------------
    # Load config
    # -------------------------------------------------
    try:
        spark = get_spark()
        config = load_config("tables.yaml")[layer]
        print("\n[SILVER REFINEMENT STARTED]")

        mode = mode or "overwrite" 

    except Exception as e:
        raise ConfigError(f"Failed to load {layer} config. Reason: {str(e)}")

    tables = {table_name: config[table_name]} if table_name else config

    # -------------------------------------------------
    # Process each table
    # -------------------------------------------------
    for name, cfg in tables.items():

        try:
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
            print(f"  Source : {source_fqn}")
            print(f"  Target : {target_fqn}")

            # -------------------------------------------------
            # Read
            # -------------------------------------------------
            df = spark.read.table(source_fqn)

            before = df.count()
            print(f"  Rows before : {before}")

            # -------------------------------------------------
            # Transform
            # -------------------------------------------------
            df = cast_columns(df, cfg.get("casts", {}))
            df = clean_data(df, cfg.get("clean_rules", {}))

            after_clean = df.count()
            print(f"  Rows after clean : {after_clean}")

            # -------------------------------------------------
            # Dedupe (only for incremental tables)
            # -------------------------------------------------
            merge_keys = _normalize_keys(cfg.get("merge_key"))
            order_col = cfg.get("dedupe_order_by", "ingestion_ts")

            if merge_keys:
                df = _dedupe(df, merge_keys, order_col)
                after_dedupe = df.count()
                print(f"  Rows after dedupe : {after_dedupe}")

            # -------------------------------------------------
            # Write strategy
            # -------------------------------------------------
            if merge_keys:
                # incremental
                merge_upsert(
                    spark_session=spark,
                    source_dataframe=df,
                    target_table_name=target_fqn,
                    merge_keys=merge_keys
                )
                print("  ✅ Merge completed")

            else:
                # full load
                write_table(
                    df=df,
                    target_table=target_fqn,
                    mode=mode
                )
                print("  ✅ Overwrite completed")

        except Exception as e:
            raise DataWriteError(
                f"[Silver] Failed writing '{target_fqn}'. Reason: {str(e)}"
            )
