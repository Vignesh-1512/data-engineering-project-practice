from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as _sum,
    avg,
    when
)

def build_seller_performance(**tables):
    """
    BR-4: Seller Performance

    Purpose:
        Evaluate seller contribution, reliability, and customer satisfaction.

    Grain:
        1 row per seller

    Source Tables:
        - fact_sales
        - fact_orders
        - fact_reviews
        - dim_sellers

    Business Questions Answered:
        - Who are the top-performing sellers?
        - Which sellers delay orders?
        - Which sellers receive poor reviews?
        - How much revenue does each seller generate?

    Key Metrics:
        - total_orders
        - total_revenue
        - avg_review_score
        - delayed_orders
        - delay_rate

    Notes:
        - Delay is identified using delivered vs estimated delivery timestamps.
        - Review data is optional and joined using left join.
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
