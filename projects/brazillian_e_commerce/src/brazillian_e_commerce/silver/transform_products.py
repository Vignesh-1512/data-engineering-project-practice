from pyspark.sql.functions import col
from brazillian_e_commerce.utils.cleaning import drop_nulls

def transform_products(df):
    df = df.select(
        col("product_id"),
        col("product_category_name")
            .alias("product_category")
    )

    return drop_nulls(df, ["product_id"])
