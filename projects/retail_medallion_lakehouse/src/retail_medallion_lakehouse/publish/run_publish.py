from retail_medallion_lakehouse.utils.spark import get_spark
from retail_medallion_lakehouse.utils.config_loader import load_config
from retail_medallion_lakehouse.utils.path_builder import build_path
from retail_medallion_lakehouse.utils.file_read import read_table
from retail_medallion_lakehouse.utils.file_hive import write_table

from retail_medallion_lakehouse.utils.publish_builders import (
    build_dimension,
    build_fact,
    build_aggregate
)


def run_publish(layer_name: str, dataset_name: str | None = None):

    print("\n===================================")
    print("🚀 PIPELINE STARTED | Layer: publish")
    print("===================================")

    spark = get_spark(app_name="retail_publish")

    try:
        config = load_config()[layer_name]
        write_mode = config["write_mode"]

        source_path = config["source_path"]
        df = spark.read.parquet(source_path)

        # Helper flags
        run_all = dataset_name is None
        run_dimensions = dataset_name == "dimensions"
        run_facts = dataset_name == "facts"
        run_aggregates = dataset_name == "aggregates"

        # =====================================================
        # DIMENSIONS
        # =====================================================
        for name, dim_config in config.get("dimensions", {}).items():

            if not (
                run_all
                or run_dimensions
                or dataset_name == name
            ):
                continue

            print(f"→ Building Dimension: {name}")

            dim_df = build_dimension(df, dim_config)

            target_table = f"{config['target_base_path']}/{dim_config['target_table']}"

            write_table(
                df=dim_df,
                target_table=target_table,
                mode=write_mode,
                format="parquet"
            )

        # =====================================================
        # FACTS
        # =====================================================
        for name, fact_config in config.get("facts", {}).items():

            if not (
                run_all
                or run_facts
                or dataset_name == name
            ):
                continue

            print(f"→ Building Fact: {name}")

            fact_df, partition_by = build_fact(df, fact_config)

            target_table = f"{config['target_base_path']}/{fact_config['target_table']}"

            write_table(
                df=fact_df,
                target_table=target_table,
                mode=write_mode,
                format="parquet",
                partition_by=partition_by if partition_by else None,
                dynamic_partition=bool(partition_by)
            )

        # =====================================================
        # AGGREGATES
        # =====================================================
        for name, agg_config in config.get("aggregates", {}).items():

            if not (
                run_all
                or run_aggregates
                or dataset_name == name
            ):
                continue

            print(f"→ Building Aggregate: {name}")

            agg_df = build_aggregate(df, agg_config)

            target_table = f"{config['target_base_path']}/{agg_config['target_table']}"

            write_table(
                df=agg_df,
                target_table=target_table,
                mode=write_mode,
                format="parquet"
            )

        print("\n[ PUBLISH COMPLETED SUCCESSFULLY ]")

    except Exception as e:
        raise Exception(f"[PUBLISH FAILED] {str(e)}")

    finally:
        print("\n🏁 PIPELINE FINISHED")
