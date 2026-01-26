from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth
)


def build_dim_date(orders_df):
    """
    Date dimension derived from orders.
    """

    df = orders_df.select(
        to_date(col("order_purchase_ts")).alias("date")
    ).dropDuplicates()

    return (
        df
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
    )
