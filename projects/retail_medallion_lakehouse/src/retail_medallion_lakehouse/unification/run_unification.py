from pyspark.sql.functions import (
    col,
    current_timestamp,
    year
)

from retail_medallion_lakehouse.utils.spark import get_spark
from retail_medallion_lakehouse.utils.config_loader import load_config
from retail_medallion_lakehouse.utils.path_builder import build_path
from retail_medallion_lakehouse.utils.file_read import read_table
from retail_medallion_lakehouse.utils.file_hive import write_table
from retail_medallion_lakehouse.utils.common_transform import (
    drop_partition_columns,
    drop_lineage_columns,
    add_lineage_column,
    safe_join
)


# ==========================================================
# 🚀 UNIFICATION LAYER (FULLY CONFIG-DRIVEN)
# ==========================================================
def run_unification(layer_name: str, dataset_name: str | None = None):

    print("\n===================================")
    print(f"🚀 PIPELINE STARTED | Layer: {layer_name}")
    print("===================================")

    spark = get_spark(app_name=f"retail_{layer_name}")

    try:
        layer_config = load_config()[layer_name]
        write_mode = layer_config["write_mode"]

        # ---------------------------------------------
        # DATASET SELECTION
        # ---------------------------------------------
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

            # ---------------------------------------------
            # BUILD TABLE PATHS
            # ---------------------------------------------
            transactions_table = build_path(
                config["catalog"],
                config["source_schema"],
                config["transactions_table"]
            )

            payments_table = build_path(
                config["catalog"],
                config["source_schema"],
                config["payments_table"]
            )

            products_table = build_path(
                config["catalog"],
                config["source_schema"],
                config["products_table"]
            )

            # ---------------------------------------------
            # READ TABLES
            # ---------------------------------------------
            transactions_df = read_table(spark, transactions_table)
            payments_df = read_table(spark, payments_table)
            products_df = read_table(spark, products_table)

            # ---------------------------------------------
            # CLEAN TECHNICAL COLUMNS
            # ---------------------------------------------
            transactions_df = (
                drop_partition_columns(transactions_df)
                .transform(drop_lineage_columns)
                .drop("ingest_ts")
            )

            payments_df = (
                drop_partition_columns(payments_df)
                .transform(drop_lineage_columns)
                .drop("ingest_ts")
            )

            products_df = (
                drop_partition_columns(products_df)
                .transform(drop_lineage_columns)
                .drop("ingest_ts")
            )

            # ---------------------------------------------
            # OPTIONAL STANDARDIZATION
            # ---------------------------------------------
            if "name" in products_df.columns:
                products_df = products_df.withColumnRenamed(
                    "name",
                    "product_name_master"
                )

            # ---------------------------------------------
            # MAP TABLE NAMES TO DATAFRAMES
            # ---------------------------------------------
            dataframe_map = {
                "transactions": transactions_df,
                "payments": payments_df,
                "products": products_df
            }

            # ---------------------------------------------
            # 🔥 CONFIG-DRIVEN JOIN LOGIC
            # ---------------------------------------------
            unified_df = transactions_df

            for join_cfg in config.get("joins", []):

                right_name = join_cfg["right"]
                join_key = join_cfg["key"]
                join_type = join_cfg.get("type", "left")

                if right_name not in dataframe_map:
                    raise Exception(
                        f"[JOIN] Unknown table '{right_name}' in config."
                    )

                right_df = dataframe_map[right_name]

                # Validate join key existence
                if isinstance(join_key, str):
                    join_key_list = [join_key]
                else:
                    join_key_list = join_key

                for key in join_key_list:
                    if key not in unified_df.columns:
                        raise Exception(
                            f"[JOIN] Key '{key}' missing in left dataframe."
                        )
                    if key not in right_df.columns:
                        raise Exception(
                            f"[JOIN] Key '{key}' missing in right dataframe."
                        )

                unified_df = safe_join(
                    unified_df,
                    right_df,
                    join_key,
                    join_type
                )

            # ---------------------------------------------
            # OVERWRITE AUDIT COLUMN
            # ---------------------------------------------
            if config.get("overwrite_audit", False):
                unified_df = unified_df.withColumn(
                    "ingest_ts",
                    current_timestamp()
                )

            # ---------------------------------------------
            # ADD LINEAGE
            # ---------------------------------------------
            unified_df = add_lineage_column(
                unified_df,
                config["lineage_value"]
            )

            # ---------------------------------------------
            # PARTITION LOGIC
            # ---------------------------------------------
            partition_by = []

            partition_col = config.get("partition_column")
            partition_type = config.get("partition_type")

            if partition_col and partition_type == "year":

                if partition_col not in unified_df.columns:
                    raise Exception(
                        f"[PARTITION] Column '{partition_col}' not found."
                    )

                unified_df = unified_df.withColumn(
                    "partition_year",
                    year(col(partition_col))
                )

                partition_by = ["partition_year"]

            # ---------------------------------------------
            # WRITE TABLE
            # ---------------------------------------------
            dynamic_flag = False
            if partition_by and write_mode == "overwrite":
                dynamic_flag = True

            target_table = build_path(
                config["catalog"],
                config["target_schema"],
                config["target_table"]
            )

            write_table(
                df=unified_df,
                target_table=target_table,
                mode=write_mode,
                format="delta",
                partition_by=partition_by if partition_by else None,
                dynamic_partition=dynamic_flag
            )

            print("  ✅ Unification write completed successfully.")

        print("\n[UNIFICATION COMPLETED]")

    except Exception as e:
        raise Exception(f"[UNIFICATION FAILED] {str(e)}")

    finally:
        print("\n🏁 PIPELINE FINISHED")
