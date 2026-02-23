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
    add_lineage_column
)
from retail_medallion_lakehouse.utils.business_calculations import (
    apply_financial_calculations,
    apply_allocation_ratio
)


# ==========================================================
# 🚀 REFINEMENT LAYER (PRODUCTION SAFE)
# ==========================================================
def run_refinement(layer_name: str, dataset_name: str | None = None):

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
            # READ UNIFIED TABLE
            # ---------------------------------------------
            source_table = build_path(
                config["catalog"],
                config["source_schema"],
                config["source_table"]
            )

            df = read_table(spark, source_table)

            # ---------------------------------------------
            # CLEAN TECHNICAL COLUMNS SAFELY
            # ---------------------------------------------
            df = (
                drop_partition_columns(df)
                .transform(drop_lineage_columns)
            )

            if "ingest_ts" in df.columns:
                df = df.drop("ingest_ts")

            # ---------------------------------------------
            # SELECT REQUIRED COLUMNS (YAML DRIVEN)
            # ---------------------------------------------
            select_cols = config.get("select_columns")

            if select_cols:
                missing_cols = [
                    c for c in select_cols if c not in df.columns
                ]

                if missing_cols:
                    raise Exception(
                        f"[REFINEMENT] Missing columns: {missing_cols}"
                    )

                df = df.select(*select_cols)

            # ---------------------------------------------
            # APPLY BUSINESS CALCULATIONS
            # ---------------------------------------------
            calculations_cfg = config.get("calculations", {})

            # 🔹 Financial Calculations
            financial_cfg = calculations_cfg.get("financial", {})
            if financial_cfg.get("enabled", False):

                required_financial_cols = [
                    "transaction_id",
                    "quantity",
                    "price",
                    "discount_amount",
                    "shipping_fee",
                    "gst_percent"
                ]

                missing_fin_cols = [
                    c for c in required_financial_cols
                    if c not in df.columns
                ]

                if missing_fin_cols:
                    raise Exception(
                        f"[REFINEMENT] Financial columns missing: {missing_fin_cols}"
                    )

                df = apply_financial_calculations(df)

            # 🔹 Allocation Ratio (Optional)
            allocation_cfg = calculations_cfg.get("allocation", {})
            if allocation_cfg.get("enabled", False):

                if "amount_charged" not in df.columns:
                    raise Exception(
                        "[REFINEMENT] amount_charged required for allocation_ratio"
                    )

                df = apply_allocation_ratio(df)

            # ---------------------------------------------
            # ADD AUDIT COLUMN
            # ---------------------------------------------
            if config.get("overwrite_audit", False):
                df = df.withColumn(
                    "ingest_ts",
                    current_timestamp()
                )

            # ---------------------------------------------
            # ADD LINEAGE
            # ---------------------------------------------
            df = add_lineage_column(
                df,
                config["lineage_value"]
            )

            # ---------------------------------------------
            # PARTITION LOGIC
            # ---------------------------------------------
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

            # ---------------------------------------------
            # WRITE REFINED TABLE
            # ---------------------------------------------
            target_table = build_path(
                config["catalog"],
                config["target_schema"],
                config["target_table"]
            )

            write_table(
                df=df,
                target_table=target_table,
                mode=write_mode,
                format="delta",
                partition_by=partition_by if partition_by else None,
                dynamic_partition=True
            )

            print("  ✅ Refinement write completed successfully.")

        print("\n[REFINEMENT COMPLETED]")

    except Exception as e:
        raise Exception(f"[REFINEMENT FAILED] {str(e)}")

    finally:
        print("\n🏁 PIPELINE FINISHED")
