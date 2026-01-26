from pyspark.sql.functions import col
from brazillian_e_commerce.utils.cleaning import drop_nulls
from brazillian_e_commerce.utils.casting import cast_columns

def transform_order_items(df):
    df = df.select(
        col("order_id"),
        col("product_id"),
        col("price"),
        col("freight_value")
    )

    df = cast_columns(df, {
        "price": "double",
        "freight_value": "double"
    })

    return drop_nulls(df, ["order_id", "product_id"])
