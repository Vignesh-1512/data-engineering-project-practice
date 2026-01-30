from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as _sum,
    when,
    to_date,
    datediff
)


def build_order_delivery_summary(**tables):
    """
    BR-2: Order Delivery Summary

    Purpose:
        Analyze order delivery behavior over time and by order status.

    Grain:
        order_date + order_status

    Source Tables:
        - fact_orders
        - dim_date

    Business Questions Answered:
        - How many orders are delivered vs cancelled?
        - What is the average delivery time?
        - Are deliveries improving over time?
        - How many orders are delayed?

    Key Metrics:
        - total_orders
        - avg_delivery_days
        - delayed_orders

    Notes:
        - Delivery delay is calculated using delivered vs purchase timestamps.
        - Delay threshold is defined in transformation logic.
    """


    # 🔑 REQUIRED LINE
    fact_orders = tables["fact_orders"]

    df = (
        fact_orders
        .withColumn("order_date", to_date(col("order_purchase_ts")))
        .withColumn(
            "delivery_days",
            datediff(
                col("order_delivered_ts"),
                col("order_purchase_ts")
            )
        )
    )

    return (
        df.groupBy(
            col("order_date"),
            col("order_status")
        )
        .agg(
            count("order_id").alias("total_orders"),
            avg("delivery_days").alias("avg_delivery_days"),
            _sum(
                when(col("delivery_days") > 7, 1).otherwise(0)
            ).alias("delayed_orders")
        )
    )
