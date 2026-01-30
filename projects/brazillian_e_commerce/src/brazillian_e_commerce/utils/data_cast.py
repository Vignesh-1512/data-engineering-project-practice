from pyspark.sql.functions import col,to_timestamp

"""
Utility module for type casting columns dynamically.
"""

def cast_columns(df, cast_map: dict):
    """
    Casts columns to specified data types.

    Args:
        df (DataFrame): Input dataframe
        cast_map (dict): Column to datatype mapping

    Returns:
        DataFrame: DataFrame with casted columns
    """

    for column, dtype in cast_map.items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dtype))
    return df
