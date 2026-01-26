def write_table(
    df,
    target_table: str,
    mode: str = "overwrite",
    format: str = "delta"
):
    """
    Writes a DataFrame to a Unity Catalog table.

    Parameters:
    - df: Spark DataFrame
    - target_table: fully qualified table name
    - mode: write mode (overwrite / append)
    - format: storage format (default delta)
    """

    (
        df.write
        .mode(mode)
        .format(format)
        .saveAsTable(target_table)
    )
