from pyspark.sql.functions import (
    col,
    sum as _sum,
    countDistinct,
    avg,
    to_date
)

def build_sales_performance(**tables):
    
    """
    Business Requirement 1 – Sales Performance.

    Answers:
        - Total revenue by product and day
        - Average order value
        - Order volume trend

    Grain:
        sales_date + product_category

    Args:
        tables (dict): Gold tables required for BR-1

    Returns:
        DataFrame: Final sales performance dataset
    """


    # 🔑 unpack tables (NO logic change)
    fact_sales = tables["fact_sales"]
    fact_orders = tables["fact_orders"]
    dim_products = tables["dim_products"]
    dim_date = tables["dim_date"]

    df = (
        fact_sales
        .join(fact_orders, "order_id", "inner")
        .join(dim_products, "product_id", "left")
        .withColumn(
            "sales_date",
            to_date(col("order_purchase_ts"))
        )
        .join(
            dim_date,
            col("sales_date") == dim_date.date,
            "left"
        )
    )

    return (
        df.groupBy(
            col("sales_date"),
            col("product_category")
        )
        .agg(
            _sum("price").alias("total_revenue"),
            countDistinct("order_id").alias("total_orders"),
            avg("price").alias("avg_order_value")
        )
    )
