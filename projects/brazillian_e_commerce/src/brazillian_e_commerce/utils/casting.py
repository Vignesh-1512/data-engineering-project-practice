from pyspark.sql.functions import col

def cast_columns(df, cast_map: dict):
    """
    Cast columns based on provided schema map.
    Example:
    {
        "price": "double",
        "freight_value": "double"
    }
    """
    for column, dtype in cast_map.items():
        df = df.withColumn(column, col(column).cast(dtype))
    return df
