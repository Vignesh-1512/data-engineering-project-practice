from pyspark.sql.functions import col, to_date, year, month, dayofmonth

"""
Generic dimension table builder utilities.
"""

def build_dim(table_name: str, **sources):

    """
    Builds a dimension table dynamically based on table name.

    Supported dimensions:
        - dim_customers
        - dim_products
        - dim_sellers
        - dim_date

    Args:
        table_name (str): Dimension table name
        sources (dict): Source DataFrames

    Returns:
        DataFrame: Dimension table

    Raises:
        ValueError: If unsupported dimension is requested
    """


    if table_name == "dim_customers":
        df = sources["customers"]
        return (
            df.select(
                col("customer_id"),
                col("customer_city"),
                col("customer_state")
            )
            .dropDuplicates(["customer_id"])
        )

    elif table_name == "dim_products":
        df = sources["products"]
        return (
            df.select(
                col("product_id"),
                col("product_category_name").alias("product_category")
            )
            .dropDuplicates(["product_id"])
        )

    elif table_name == "dim_sellers":
        df = sources["sellers"]
        return (
            df.select(
                col("seller_id"),
                col("seller_city"),
                col("seller_state")
            )
            .dropDuplicates(["seller_id"])
        )

    elif table_name == "dim_date":
        orders_df = sources["orders"]
        df = orders_df.select(
            to_date(col("order_purchase_ts")).alias("date")
        ).dropDuplicates()

        return (
            df
            .withColumn("year", year("date"))
            .withColumn("month", month("date"))
            .withColumn("day", dayofmonth("date"))
        )

    else:
        raise ValueError(f"Unsupported DIM table: {table_name}")
