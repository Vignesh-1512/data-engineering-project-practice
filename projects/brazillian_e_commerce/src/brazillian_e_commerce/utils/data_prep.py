from pyspark.sql.functions import col


def clean_data(df, rules: dict):
    """
    Apply cleaning & standardization rules.

    rules example:
    {
        "drop_nulls": ["order_id", "customer_id"],
        "rename": {
            "order_purchase_timestamp": "order_purchase_ts"
        }
    }
    """

    # 1️⃣ Rename columns (standardization)
    rename_map = rules.get("rename", {})
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # 2️⃣ Drop NULLs
    null_cols = rules.get("drop_nulls", [])
    for c in null_cols:
        if c in df.columns:
            df = df.filter(col(c).isNotNull())

    return df
