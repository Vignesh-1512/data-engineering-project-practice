import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lit, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from collections import Counter


# ==========================================================
# ✂ TRIM STRING COLUMNS
# ==========================================================
def trim_string_columns(df: DataFrame) -> DataFrame:
    """
    Trims all string columns in dataframe.
    """
    string_cols = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString() == "string"
    ]

    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    return df


# ==========================================================
# 🚫 NOT NULL VALIDATION
# ==========================================================
def apply_not_null_validation(df: DataFrame, columns: list) -> DataFrame:
    """
    Filters rows where specified columns are NULL.
    """
    for c in columns:
        if c not in df.columns:
            raise Exception(f"[VALIDATION] Column '{c}' not found.")
        df = df.filter(col(c).isNotNull())

    return df


# ==========================================================
# ➕ POSITIVE VALUE VALIDATION
# ==========================================================
def apply_positive_validation(df: DataFrame, columns: list) -> DataFrame:
    """
    Keeps rows where specified numeric columns > 0.
    """
    for c in columns:
        if c not in df.columns:
            raise Exception(f"[VALIDATION] Column '{c}' not found.")
        df = df.filter(col(c) > 0)

    return df


# ==========================================================
# 🔁 DEDUPLICATE (KEEP LATEST RECORD)
# ==========================================================
def deduplicate_latest(
        df: DataFrame,
        primary_keys: list,
        order_column: str
) -> DataFrame:
    """
    Deduplicate based on primary keys,
    keeping latest record using order_column.
    """

    if not primary_keys:
        raise Exception("[DEDUP] Primary keys required.")

    if order_column not in df.columns:
        raise Exception(f"[DEDUP] Order column '{order_column}' not found.")

    window_spec = Window.partitionBy(*primary_keys) \
                        .orderBy(col(order_column).desc())

    df = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") == 1) \
           .drop("rn")

    return df


# ==========================================================
# 🧬 CUMULATIVE LINEAGE
# ==========================================================
def add_lineage_column(df, current_layer: str):

    layer_order =[
        "pre_landing",
        "landing",
        "unification",
        "refinement",
        "publish"
    ]

    # Do NOT add lineage for pre_landing
    if current_layer == "pre_landing":
        return df

    if current_layer not in layer_order:
        raise Exception(f"Unknown layer: {current_layer}")

    # Build lineage up to current layer
    idx = layer_order.index(current_layer)
    lineage_value = " -> ".join(layer_order[: idx + 1])

    df = df.withColumn("lineage", lit(lineage_value))

    return df


# ==========================================================
# 🧹 DROP PARTITION COLUMNS (TECHNICAL CLEANER)
# ==========================================================
def drop_partition_columns(df: DataFrame) -> DataFrame:
    """
    Drops technical partition columns like partition_year,
    partition_month, partition_day if present.
    """

    partition_cols = [
        c for c in df.columns
        if c.startswith("partition_")
    ]

    if partition_cols:
        df = df.drop(*partition_cols)

    return df

#=================================================================
#   Drop all Lineage columns
#=================================================================

def drop_lineage_columns(df: DataFrame) -> DataFrame:
    lineage_cols = [c for c in df.columns if re.match(r"^lineage", c)]
    return df.drop(*lineage_cols)



# ==========================================================
# 🔥 GENERIC SAFE JOIN (NO HARDCODING)
# ==========================================================
def safe_join(left_df, right_df, join_cols, join_type="left"):

    if isinstance(join_cols, str):
        join_cols = [join_cols]

    left_cols = set(left_df.columns)
    right_cols = set(right_df.columns)

    overlap_cols = left_cols.intersection(right_cols)
    duplicate_cols = overlap_cols - set(join_cols)

    if duplicate_cols:
        print(f"[SAFE_JOIN] Dropping duplicate columns from right_df: {duplicate_cols}")
        right_df = right_df.drop(*duplicate_cols)

    return left_df.join(right_df, join_cols, join_type)


# ==========================================================
# 🕒 FILTER LATEST BATCH BASED ON INGEST_TS
# ==========================================================
def filter_latest_batch(df, ingestion_column="ingest_ts"):
    """
    Keeps only records from latest ingestion batch.
    """

    if ingestion_column not in df.columns:
        raise Exception(
            f"[BATCH FILTER] Column '{ingestion_column}' not found."
        )

    max_ts = (
        df.select(spark_max(ingestion_column).alias("max_ts"))
          .collect()[0]["max_ts"]
    )

    return df.filter(col(ingestion_column) == max_ts)