from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


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
# 🧬 LINEAGE (Previous Layers Only)
# ==========================================================
def add_lineage_column(df: DataFrame, previous_layer: str) -> DataFrame:
    """
    Adds lineage columns dynamically.
    Stores only previous layer names.
    
    Example:
    Landing → lineage_1 = pre_landing
    Unification → lineage_1 = pre_landing
                    lineage_2 = landing
    """

    lineage_cols = sorted([c for c in df.columns if c.startswith("lineage_")])

    if not lineage_cols:
        # First lineage column
        df = df.withColumn("lineage_1", lit(previous_layer))
    else:
        next_index = len(lineage_cols) + 1
        df = df.withColumn(f"lineage_{next_index}", lit(previous_layer))

    return df
