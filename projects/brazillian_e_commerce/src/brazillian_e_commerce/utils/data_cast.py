from pyspark.sql.functions import col,to_timestamp


def cast_columns(df, cast_map: dict):
    """
    Cast columns based on provided schema map.

    Example:
    {
        "price": "double",
        "freight_value": "double",
        "order_purchase_timestamp": "timestamp"
    }
    """
    for column, dtype in cast_map.items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dtype))
    return df
