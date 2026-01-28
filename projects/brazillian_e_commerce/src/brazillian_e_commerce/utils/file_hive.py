def write_table(
    df,
    target_table: str,
    mode: str = "overwrite",
    format: str = "delta",
    overwrite_schema: bool = False
):
    writer = (
        df.write
        .mode(mode)
        .format(format)
    )

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    writer.saveAsTable(target_table)
