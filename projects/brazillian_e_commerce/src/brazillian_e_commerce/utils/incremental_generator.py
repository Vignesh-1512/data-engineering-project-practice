import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql.types import *


# =====================================================
# Optional business rules (only for meaningful columns)
# =====================================================

COLUMN_RULES = {
    "order_status": ["processing", "shipped", "delivered", "cancelled"],
    "review_score": [1, 2, 3, 4, 5]
}


# =====================================================
# Helpers
# =====================================================

def _uuid():
    # 32 hex chars, no hyphen (same as your dataset)
    return uuid.uuid4().hex


def _random_ts():
    return datetime.now() - timedelta(minutes=random.randint(0, 10000))


def _value_by_type(dtype):
    """
    Generic fallback generator based only on datatype.
    Works automatically for any new column.
    """

    if isinstance(dtype, StringType):
        return _uuid()

    if isinstance(dtype, (IntegerType, LongType, ShortType)):
        return random.randint(1, 1000)

    if isinstance(dtype, (DoubleType, FloatType, DecimalType)):
        return round(random.uniform(1, 5000), 2)

    if isinstance(dtype, BooleanType):
        return random.choice([True, False])

    if isinstance(dtype, TimestampType):
        return _random_ts()

    return None


# =====================================================
# MAIN GENERATOR
# =====================================================

def input_data_generator(
    spark,
    schema,
    target_table: str,
    merge_key: str,
    rows: int = 5
):
    """
    Generates synthetic incremental rows.

    Row 1  -> UPDATE  (same order_id + same customer_id)
    Row 2+ -> INSERT  (all random)

    Fully dynamic and schema-driven.
    """

    data = []
    existing_row = None

    # ------------------------------------------
    # fetch one existing row (for update testing)
    # ------------------------------------------
    if spark.catalog.tableExists(target_table):
        sample = spark.read.table(target_table).limit(1).collect()
        if sample:
            existing_row = sample[0].asDict()

    # ------------------------------------------
    # generate rows
    # ------------------------------------------
    for i in range(rows):

        row = []

        for field in schema:
            col = field.name

            # =========================================
            # ROW 1 → UPDATE
            # keep only order_id + customer_id same
            # everything else regenerated
            # =========================================
            if i == 0 and existing_row:

                if (
                    isinstance(merge_key, list) and col in merge_key
                ) or (
                    isinstance(merge_key, str) and col == merge_key
                ):
                    value = existing_row[col]

                elif col == "customer_id" and col in existing_row:
                    value = existing_row[col]

                elif col in COLUMN_RULES:
                    value = random.choice(COLUMN_RULES[col])

                else:
                    value = _value_by_type(field.dataType)

            # =========================================
            # ROW 2+ → INSERT
            # =========================================
            else:

                if col in COLUMN_RULES:
                    value = random.choice(COLUMN_RULES[col])
                else:
                    value = _value_by_type(field.dataType)

            row.append(value)

        data.append(tuple(row))

    return spark.createDataFrame(data, schema)
