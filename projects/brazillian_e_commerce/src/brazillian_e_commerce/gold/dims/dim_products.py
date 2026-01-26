from pyspark.sql.functions import col


def build_dim_products(products_df):
    """
    Product dimension.
    One row per product.
    """

    return (
        products_df
        .select(
            col("product_id"),
            col("product_category")
        )
        .dropDuplicates(["product_id"])
    )
