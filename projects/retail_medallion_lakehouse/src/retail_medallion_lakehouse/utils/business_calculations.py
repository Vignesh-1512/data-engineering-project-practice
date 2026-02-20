from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.window import Window


# ==========================================================
# 💰 FINANCIAL CALCULATIONS (CORRECTED)
# ==========================================================
def apply_financial_calculations(df):

    # Net after discount
    df = df.withColumn(
        "net_amount",
        col("amount_gross") - col("discount_amount")
    )

    # GST using column value
    df = df.withColumn(
        "gst_amount",
        col("net_amount") * (col("gst_percent") / 100)
    )

    # Final invoice amount
    df = df.withColumn(
        "final_invoice_amount",
        col("net_amount") + col("gst_amount") + col("shipping_fee")
    )

    return df


# ==========================================================
# 🔢 PAYMENT ALLOCATION RATIO (ONLY IF NEEDED)
# ==========================================================
def apply_allocation_ratio(df):

    # Use amount_charged instead of non-existing payment_amount
    window_spec = Window.partitionBy("transaction_id")

    df = df.withColumn(
        "total_payment_per_txn",
        spark_sum("amount_charged").over(window_spec)
    )

    df = df.withColumn(
        "allocation_ratio",
        col("amount_charged") / col("total_payment_per_txn")
    ).drop("total_payment_per_txn")

    return df
