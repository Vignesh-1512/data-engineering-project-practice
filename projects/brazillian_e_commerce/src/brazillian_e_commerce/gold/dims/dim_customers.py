from pyspark.sql.functions import col


def build_dim_customers(customers_df):
    """
    Customer dimension.
    One row per customer.
    """

    return (
        customers_df
        .select(
            col("customer_id"),
            col("customer_city"),
            col("customer_state")
        )
        .dropDuplicates(["customer_id"])
    )
