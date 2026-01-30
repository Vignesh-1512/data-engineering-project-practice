from pyspark.sql.functions import col
"""
Utility module for applying data quality rules.
"""

def clean_data(df, rules: dict):
    """
    Applies data cleaning rules such as renaming and null filtering.

    Supported rules:
        - rename
        - drop_nulls

    Args:
        df (DataFrame): Input dataframe
        rules (dict): Cleaning rules from YAML

    Returns:
        DataFrame: Cleaned dataframe
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
