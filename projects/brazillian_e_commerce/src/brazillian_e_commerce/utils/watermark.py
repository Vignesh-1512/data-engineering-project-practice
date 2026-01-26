def get_last_watermark(spark, table_name: str, watermark_col: str):
    """
    Returns max watermark value from target table.
    """

    if not spark.catalog.tableExists(table_name):
        return None

    return (
        spark.read.table(table_name)
        .selectExpr(f"max({watermark_col}) as max_value")
        .collect()[0]["max_value"]
    )
