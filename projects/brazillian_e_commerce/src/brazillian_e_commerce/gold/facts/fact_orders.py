from pyspark.sql.functions import col


def build_fact_orders(orders_df):
    """
    FACT: Orders
    Grain: 1 row per order
    """

    return (
        orders_df
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_status"),
            col("order_purchase_ts")
        )
    )
