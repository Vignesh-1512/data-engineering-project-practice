from pyspark.sql.functions import col


def build_fact(table_name: str, **sources):
    """
    Generic FACT builder.
    """

    if table_name == "fact_orders":
        df = sources["orders"]
        return df.select(
            col("order_id"),
            col("customer_id"),
            col("order_status"),
            col("order_purchase_ts"),
            col("order_delivered_ts"),
            col("order_estimated_delivery_ts")
        )

    elif table_name == "fact_sales":
        df = sources["order_items"]
        return df.select(
            col("order_id"),
            col("product_id"),
            col("seller_id"),
            col("price"),
            col("freight_value"),
            (col("price") + col("freight_value")).alias("revenue")
        )

    elif table_name == "fact_reviews":
        df = sources["reviews"]
        return df.select(
            col("review_id"),
            col("order_id"),
            col("review_score")
        )

    else:
        raise ValueError(f"Unsupported FACT table: {table_name}")
