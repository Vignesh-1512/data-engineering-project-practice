from pyspark.sql.functions import col


def build_fact_sales(order_items_df):
    """
    Sales fact sales.
    Grain: one row per order item.
    """

    return (
        order_items_df
        .select(
            col("order_id"),
            col("product_id"),
            col("price"),
            col("freight_value")
        )
    )
