from pyspark.sql.functions import (
    col,
    count,
    avg,
    when,
    datediff,
    to_date
)

def build_review_satisfaction(**tables):
    """
    BR-7: Customer Satisfaction and Reviews

    Purpose:
        Correlate customer satisfaction with products, sales, and order behavior.

    Grain:
        customer + product

    Source Tables:
        - fact_reviews
        - fact_orders
        - fact_sales
        - dim_products
        - dim_date

    Business Questions Answered:
        - Which products receive poor reviews?
        - How does revenue relate to customer satisfaction?
        - Are delayed deliveries impacting review scores?

    Key Metrics:
        - avg_review_score
        - total_reviews
        - total_revenue

    Notes:
        - Review score aggregation is performed at customer-product level.
        - Designed for customer experience and quality analysis.
    """


    fact_reviews = tables["fact_reviews"]
    fact_orders  = tables["fact_orders"]
    fact_sales   = tables["fact_sales"]
    dim_products = tables["dim_products"]
    dim_date     = tables["dim_date"]

    # 1️⃣ Join reviews → orders → sales → products
    df = (
        fact_reviews
        .join(fact_orders, "order_id", "inner")
        .join(fact_sales, "order_id", "inner")
        .join(dim_products, "product_id", "left")
    )

    # 2️⃣ Derive review_date + delivery_days
    df = (
        df
        .withColumn("review_date", to_date(col("order_purchase_ts")))
        .withColumn(
            "delivery_days",
            datediff(
                col("order_delivered_ts"),
                col("order_purchase_ts")
            )
        )
        .join(
            dim_date,
            col("review_date") == dim_date.date,
            "left"
        )
    )

    # 3️⃣ Aggregate
    result = (
        df
        .groupBy(
            col("review_date"),
            col("product_id"),
            col("product_category")
        )
        .agg(
            count("review_id").alias("total_reviews"),
            avg("review_score").alias("avg_review_score"),
            count(
                when(col("review_score") >= 4, True)
            ).alias("positive_reviews"),
            count(
                when(col("review_score") <= 2, True)
            ).alias("negative_reviews"),
            avg("delivery_days").alias("avg_delivery_days")
        )
    )

    return result
