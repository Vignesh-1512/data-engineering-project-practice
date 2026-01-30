from pyspark.sql.functions import col

"""
Generic fact table builder utilities.
"""


def build_fact(table_name: str, **sources):
    """
    Builds a fact table dynamically based on table name.

    Supported facts:
        - fact_orders
        - fact_sales
        - fact_reviews
        - fact_payments

    Args:
        table_name (str): Fact table name
        sources (dict): Source DataFrames

    Returns:
        DataFrame: Fact table

    Raises:
        ValueError: If unsupported fact is requested
    """


    if table_name == "fact_orders":
    
        """
        Builds the fact_orders table.

        Grain:
            1 row per order

        Source:
            silver.orders

        Columns:
            order_id
            customer_id
            order_status
            order_purchase_ts
            order_delivered_ts
            order_estimated_delivery_ts

        Args:
            df (DataFrame): Silver orders dataframe

        Returns:
            DataFrame: Gold fact_orders table
        """

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
    elif table_name == "fact_payments":
        df = sources["payments"]
        return df.select(
            col("order_id"),
            col("payment_sequential"),
            col("payment_type"),
            col("payment_installments"),
            col("payment_value")
        )


    else:
        raise ValueError(f"Unsupported FACT table: {table_name}")
