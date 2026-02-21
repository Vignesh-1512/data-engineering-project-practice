from pyspark.sql import DataFrame


def write_table(
    df: DataFrame,
    target_table: str,
    mode: str,
    format: str = "delta",
    partition_by: list[str] | None = None,
    dynamic_partition: bool = False
):
    """
    Writes DataFrame to Unity Catalog managed table.

    Supports:
    - append
    - overwrite
    - dynamic partition overwrite (for partitioned tables)
    """

    try:
        if mode not in ["append", "overwrite"]:
            raise ValueError(f"Invalid write mode: {mode}")

        if format not in ["delta", "parquet"]:
            raise ValueError(f"Unsupported format: {format}")

        print("\n💾 Writing Data")
        print(f"   Target Table       : {target_table}")
        print(f"   Format             : {format}")
        print(f"   Mode               : {mode}")
        print(f"   Partition Columns  : {partition_by}")
        print(f"   Dynamic Overwrite  : {dynamic_partition}")
        
        spark = df.sparkSession

        # ------------------------------------------------
        # 🔥 DEV FIX: Drop table before overwrite
        # ------------------------------------------------
        if mode == "overwrite":
            print("   Dropping existing table (dev-safe overwrite)")
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        writer = df.write.format(format).mode(mode)
        
        # ---------------------------------------
        # Dynamic Partition Overwrite
        # ---------------------------------------
        if dynamic_partition and mode == "overwrite":
            if not partition_by:
                raise ValueError(
                    "Dynamic partition overwrite requires partition_by columns."
                )
            writer = writer.option("partitionOverwriteMode", "dynamic")

        # ---------------------------------------
        # Partitioning
        # ---------------------------------------
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.saveAsTable(target_table)

        print("✅ Write completed successfully.")

    except Exception as e:
        raise Exception(
            f"[WRITE_TABLE] Failed writing to {target_table}. "
            f"Reason: {str(e)}"
        )
