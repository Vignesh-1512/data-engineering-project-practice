from pyspark.sql.functions import col, to_timestamp
from brazillian_e_commerce.utils.cleaning import drop_nulls

def transform_orders(df):
    df = (
        df.select(
            col("order_id"),
            col("customer_id"),
            col("order_status"),
            to_timestamp("order_purchase_timestamp")
                .alias("order_purchase_ts")
        )
    )

    return drop_nulls(df, ["order_id"])
