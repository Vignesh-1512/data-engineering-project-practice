"""
Utility module for reading source tables.

Supports full and incremental reads based on watermark logic.
Used mainly by the Bronze ingestion layer.
"""


def read_table(
    spark,
    source_table: str,
    load_type: str = "full",
    watermark_col: str = None,
    target_table: str = None
):
    """
    Reads data from a source table.

    Supports:
    - Full load
    - Incremental load using watermark column

    Args:
        spark (SparkSession): Active Spark session
        source_table (str): Fully qualified source table name
        load_type (str): full | incremental
        watermark_col (str | None): Column used for incremental filtering
        target_table (str | None): Target table for watermark lookup

    Returns:
        DataFrame: Loaded dataframe

    Raises:
        ValueError: If incremental load is requested without watermark info
    """


    df = spark.read.table(source_table)

    if load_type == "incremental":
        if watermark_col is None or target_table is None:
            raise ValueError("Incremental load requires watermark_col and target_table")

        from brazillian_e_commerce.utils.watermark import get_last_watermark

        last_value = get_last_watermark(spark, target_table, watermark_col)

        if last_value:
            df = df.filter(f"{watermark_col} > '{last_value}'")

    return df
