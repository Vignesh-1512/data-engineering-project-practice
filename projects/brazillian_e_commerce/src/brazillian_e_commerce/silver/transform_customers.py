from pyspark.sql.functions import col
from brazillian_e_commerce.utils.cleaning import drop_nulls

def transform_customers(df):
    df = df.select(
        col("customer_id"),
        col("customer_city"),
        col("customer_state")
    )

    return drop_nulls(df, ["customer_id"])
