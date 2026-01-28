def read_table(
    spark,
    source_table: str,
    load_type: str = "full",
    watermark_col: str = None,
    target_table: str = None
):
    """
    Reads data from source table.
    Supports full and incremental loads.
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
