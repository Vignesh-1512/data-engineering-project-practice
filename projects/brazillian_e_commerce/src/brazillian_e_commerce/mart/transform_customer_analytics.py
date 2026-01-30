from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as _sum,
    min as _min,
    max as _max,
    when,
    to_date
)

def build_customer_analytics(**tables):
    """
    BR-3: Customer Analytics

    Purpose:
        Provide customer-level purchasing behavior and value insights.

    Grain:
        1 row per customer

    Source Tables:
        - fact_orders
        - fact_sales
        - dim_customers
        - dim_date

    Business Questions Answered:
        - How many orders does each customer place?
        - What is the total and average revenue per customer?
        - Who are repeat vs new customers?
        - What is the first and last purchase date per customer?

    Key Metrics:
        - total_orders
        - total_revenue
        - avg_order_value
        - customer_type (New / Repeat)
        - first_order_date
        - last_order_date

    Notes:
        - Revenue is derived from price + freight_value.
        - Customer type is classified based on order count.
    """


    # 🔑 unpack tables (NO logic change)
    fact_sales = tables["fact_sales"]
    fact_orders = tables["fact_orders"]
    dim_customers = tables["dim_customers"]
    dim_date = tables["dim_date"]

    # -------------------------------
    # Prepare orders
    # -------------------------------
    orders = (
        fact_orders
        .withColumn("order_date", to_date(col("order_purchase_ts")))
    )

    # -------------------------------
    # Prepare sales
    # -------------------------------
    sales = (
        fact_sales
        .withColumn(
            "revenue",
            col("price") + col("freight_value")
        )
    )

    # -------------------------------
    # Join facts
    # -------------------------------
    sales_orders = (
        sales
        .join(orders, "order_id", "inner")
        .join(dim_date, orders.order_date == dim_date.date, "left")
    )

    # -------------------------------
    # Aggregate per customer
    # -------------------------------
    customer_metrics = (
        sales_orders
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            _sum("revenue").alias("total_revenue"),
            _min("date").alias("first_order_date"),
            _max("date").alias("last_order_date")
        )
    )

    # -------------------------------
    # Join customer dimension
    # -------------------------------
    result = (
        customer_metrics
        .join(dim_customers, "customer_id", "left")
        .withColumn(
            "avg_order_value",
            col("total_revenue") / col("total_orders")
        )
        .withColumn(
            "customer_type",
            when(col("total_orders") > 1, "Repeat").otherwise("New")
        )
        .select(
            col("customer_id"),
            col("customer_city"),
            col("customer_state"),
            col("total_orders"),
            col("total_revenue"),
            col("avg_order_value"),
            col("first_order_date"),
            col("last_order_date"),
            col("customer_type")
        )
    )

    return result
