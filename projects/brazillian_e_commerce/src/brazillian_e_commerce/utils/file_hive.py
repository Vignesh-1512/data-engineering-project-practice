"""
Utility module for writing Spark DataFrames to Hive/Unity Catalog tables.
"""

def write_table(
    df,
    target_table: str,
    mode: str = "overwrite",
    format: str = "delta",
    overwrite_schema: bool = False
):
    """
    Writes a DataFrame to a Hive/Unity Catalog table.

    Args:
        df (DataFrame): Data to be written
        target_table (str): Fully qualified target table name
        mode (str): overwrite | append
        overwrite_schema (bool): Whether to overwrite schema

    Raises:
        DataWriteError: If write operation fails
    """


    writer = (
        df.write
        .mode(mode)
        .format(format)
    )

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    writer.saveAsTable(target_table)
