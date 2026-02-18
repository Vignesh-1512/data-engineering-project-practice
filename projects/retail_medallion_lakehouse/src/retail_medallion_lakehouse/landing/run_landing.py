from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    to_timestamp
)

from retail_medallion_lakehouse.utils.spark import get_spark
from retail_medallion_lakehouse.utils.config_loader import load_config
from retail_medallion_lakehouse.utils.path_builder import build_path
from retail_medallion_lakehouse.utils.file_read import read_table
from retail_medallion_lakehouse.utils.file_hive import write_table

from retail_medallion_lakehouse.utils.common_transform import (
    trim_string_columns,
    apply_not_null_validation,
    apply_positive_validation,
    deduplicate_latest,
    add_lineage_column
)


# ==========================================================
# 🚀 LANDING LAYER
# ==========================================================
def run_landing(layer_name: str, dataset_name: str | None = None):

    print("\n===================================")
    print(f"🚀 PIPELINE STARTED | Layer: {layer_name}")
    print("===================================")

    spark = get_spark(app_name=f"retail_{layer_name}")

    try:
        layer_config = load_config()[layer_name]
        print("\n[LANDING STARTED]")

        # -------------------------------------------------
        # DATASET SELECTION
        # -------------------------------------------------
        if dataset_name:
            datasets_to_process = {dataset_name: layer_config[dataset_name]}
        else:
            datasets_to_process = {
                name: config
                for name, config in layer_config.items()
                if name != "write_mode"
            }

        for dataset, config in datasets_to_process.items():

            print(f"\n→ Processing dataset : {dataset}")

            write_mode = layer_config["write_mode"]

            # -------------------------------------------------
            # BUILD TABLE NAMES
            # -------------------------------------------------
            source_table = build_path(
                config["catalog"],
                config["source_schema"],
                config["source_table"]
            )

            target_table = build_path(
                config["catalog"],
                config["target_schema"],
                config["target_table"]
            )

            # -------------------------------------------------
            # READ SOURCE USING COMMON FUNCTION
            # -------------------------------------------------
            df = read_table(spark, source_table)

            # -------------------------------------------------
            # CAST DATA TYPES
            # -------------------------------------------------
            for column_name, target_type in config.get("casts", {}).items():

                if column_name not in df.columns:
                    raise Exception(f"[CAST] Column '{column_name}' not found.")

                if target_type == "timestamp":
                    df = df.withColumn(
                        column_name,
                        to_timestamp(col(column_name))
                    )
                else:
                    df = df.withColumn(
                        column_name,
                        col(column_name).cast(target_type)
                    )

            # -------------------------------------------------
            # TRANSFORMS
            # -------------------------------------------------
            if config.get("trim_strings", False):
                df = trim_string_columns(df)

            df = apply_not_null_validation(
                df,
                config.get("not_null_columns", [])
            )

            df = apply_positive_validation(
                df,
                config.get("positive_columns", [])
            )

            # -------------------------------------------------
            # OVERWRITE AUDIT COLUMN
            # -------------------------------------------------
            if config.get("overwrite_audit", False):
                df = df.withColumn("ingest_ts", current_timestamp())

            # -------------------------------------------------
            # DEDUPLICATION (KEEP LATEST)
            # -------------------------------------------------
            if config.get("primary_keys"):
                df = deduplicate_latest(
                    df,
                    primary_keys=config.get("primary_keys"),
                    order_column="ingest_ts"
                )

            # -------------------------------------------------
            # LINEAGE (Previous Layers Only)
            # -------------------------------------------------
            df = add_lineage_column(df, config["lineage_value"])

            # -------------------------------------------------
            # PARTITION LOGIC
            # -------------------------------------------------
            partition_by = []

            partition_col = config.get("partition_column")
            partition_type = config.get("partition_type")

            if partition_col and partition_type == "year":

                if partition_col not in df.columns:
                    raise Exception(
                        f"[PARTITION] Column '{partition_col}' not found."
                    )

                df = df.withColumn(
                    "partition_year",
                    year(col(partition_col))
                )

                partition_by = ["partition_year"]

            # -------------------------------------------------
            # SKIP EMPTY DATA
            # -------------------------------------------------
            if not df.head(1):
                print("⚠ No valid data after transformations. Skipping write.")
                continue

            # -------------------------------------------------
            # WRITE USING COMMON FUNCTION
            # -------------------------------------------------
            write_table(
                df=df,
                target_table=target_table,
                mode=write_mode,
                format="delta",
                partition_by=partition_by,
                dynamic_partition=True
            )

            print("  Landing write completed successfully.")

        print("\n[LANDING COMPLETED]")

    except Exception as e:
        raise Exception(f"[LANDING FAILED] {str(e)}")

