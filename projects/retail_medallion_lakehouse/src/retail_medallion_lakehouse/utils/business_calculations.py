from pyspark.sql.functions import col, sum as spark_sum, round
from pyspark.sql.window import Window


def apply_financial_calculations(df):

    window_txn = Window.partitionBy("transaction_id")

    # ------------------------------------------------------
    # 1️⃣ Item amount (GST inclusive)
    # ------------------------------------------------------
    df = df.withColumn(
        "calculated_item_amount",
        round(col("quantity") * col("price"), 4)
    )

    # ------------------------------------------------------
    # 2️⃣ Extract GST portion from inclusive price
    # ------------------------------------------------------
    df = df.withColumn(
        "calculated_gst",
        round(
            col("calculated_item_amount")
            - (col("calculated_item_amount") /
               (1 + (col("gst_percent") / 100))),
            4
        )
    )

    # ------------------------------------------------------
    # 3️⃣ Total item amount per transaction
    # ------------------------------------------------------
    df = df.withColumn(
        "total_item_amount",
        spark_sum("calculated_item_amount").over(window_txn)
    )

    # ------------------------------------------------------
    # 4️⃣ Allocation ratio
    # ------------------------------------------------------
    df = df.withColumn(
        "calculated_allocation_ratio",
        round(
            col("calculated_item_amount") / col("total_item_amount"),
            6
        )
    )

    # ------------------------------------------------------
    # 5️⃣ Discount allocation
    # ------------------------------------------------------
    df = df.withColumn(
        "calculated_discount_allocation",
        round(
            col("discount_amount") * col("calculated_allocation_ratio"),
            4
        )
    )

    # ------------------------------------------------------
    # 6️⃣ Shipping allocation
    # ------------------------------------------------------
    df = df.withColumn(
        "calculated_shipping_allocation",
        round(
            col("shipping_fee") * col("calculated_allocation_ratio"),
            4
        )
    )

    # ------------------------------------------------------
    # 7️⃣ Final allocated amount (RECONCILED)
    # ------------------------------------------------------
    df = df.withColumn(
        "calculated_final_amount",
        round(
            col("calculated_item_amount")
            - col("calculated_discount_allocation")
            + col("calculated_shipping_allocation"),
            4
        )
    )

    df = df.drop("total_item_amount")

    return df

# ==========================================================
# 🔢 DISABLE OLD PAYMENT-LEVEL ALLOCATION
# ==========================================================
def apply_allocation_ratio(df):
    # Not required anymore (kept for compatibility)
    return df