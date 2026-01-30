from pyspark.sql.functions import col, sum as _sum, countDistinct, to_date

def build_payment_analytics(**tables):
    """
    BR-6: Payment Analytics

    Purpose:
        Understand customer payment behavior and payment method usage.

    Grain:
        order_date + payment_type

    Source Tables:
        - fact_payments
        - fact_orders
        - dim_date

    Business Questions Answered:
        - Which payment methods are most used?
        - What is the total payment value per method?
        - How does payment behavior change over time?

    Key Metrics:
        - total_payments
        - total_payment_value
        - avg_payment_value

    Notes:
        - Payment dates are derived from order purchase timestamp.
        - Supports multiple payment methods per order.
    """


    fact_payments = tables["fact_payments"]
    fact_orders = tables["fact_orders"]
    dim_date = tables["dim_date"]

    df = (
        fact_payments
        .join(fact_orders, "order_id", "inner")
        .withColumn("payment_date", to_date(col("order_purchase_ts")))
        .join(dim_date, col("payment_date") == dim_date.date, "left")
    )

    return (
        df.groupBy("payment_date", "payment_type")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            _sum("payment_value").alias("total_payment_value")
        )
    )
