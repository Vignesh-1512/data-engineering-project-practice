from pyspark.sql.functions import (
    col,
    sum as _sum,
    countDistinct,
    to_date
)

def build_product_category_performance(**tables):
    """
    BR-5: Product Category Performance

    Purpose:
        Analyze sales trends and performance across product categories.

    Grain:
        sales_date + product_category

    Source Tables:
        - fact_sales
        - fact_orders
        - dim_products
        - dim_date

    Business Questions Answered:
        - Which product categories generate the highest revenue?
        - How do product category sales trend over time?
        - What is the average order value per category?

    Key Metrics:
        - total_revenue
        - total_orders
        - avg_order_value

    Notes:
        - Sales date is derived from order purchase timestamp.
        - Category name is sourced from product dimension.
    """


    fact_sales=tables["fact_sales"]
    fact_orders=tables["fact_orders"]
    dim_products=tables["dim_products"]
    dim_date=tables["dim_date"]

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

    result = (
        df.groupBy(
            col("sales_date"),
            col("product_category")
        )
        .agg(
            _sum("revenue").alias("total_revenue"),
            countDistinct("order_id").alias("total_orders")
        )
        .withColumn(
            "avg_order_value",
            col("total_revenue") / col("total_orders")
        )
    )

    return result
