from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as _sum,
    avg,
    when
)

def build_seller_performance(**tables):
    """
    FINAL: Seller Performance (BR-4)
    Grain: 1 row per seller
    """

    fact_sales = tables["fact_sales"]
    fact_orders = tables["fact_orders"]
    fact_reviews = tables["fact_reviews"]
    dim_sellers = tables["dim_sellers"]
    # dim_date may exist — safely ignored

    df = (
        fact_sales
        .join(fact_orders, "order_id", "inner")
        .join(fact_reviews, "order_id", "left")
        .join(dim_sellers, "seller_id", "left")
    )

    df = df.withColumn(
        "is_delayed",
        when(
            col("order_delivered_ts") > col("order_estimated_delivery_ts"),
            1
        ).otherwise(0)
    )

    result = (
        df
        .groupBy(
            "seller_id",
            "seller_city",
            "seller_state"
        )
        .agg(
            countDistinct("order_id").alias("total_orders"),
            _sum("revenue").alias("total_revenue"),
            avg("review_score").alias("avg_review_score"),
            _sum("is_delayed").alias("delayed_orders")
        )
        .withColumn(
            "delay_rate",
            col("delayed_orders") / col("total_orders")
        )
    )

    return result
