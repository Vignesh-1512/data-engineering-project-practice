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
    FINAL: Order Delivery Summary (BR-2)
    Grain: order_date + order_status
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
