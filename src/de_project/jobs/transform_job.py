import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def transform_run(dataset: str, process_date: str):
    spark = SparkSession.builder.getOrCreate()

    safe_date = process_date.replace("-", "_")
    source_view = f"{dataset}_{safe_date}"
    target_view = f"{dataset}_transformed_{safe_date}"

    print(f"Transforming data from temp view: {source_view}")

    # 🔹 SQL-based transform (no table, no DB)
    df_transformed = spark.sql(f"""
        SELECT
            event_id,
            user_id,
            event_type,
            amount,
            event_time,
            DATE(event_time) AS event_date,
            CASE
                WHEN amount >= 1000 THEN 'HIGH'
                WHEN amount >= 500 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS amount_category
        FROM {source_view}
        WHERE event_type IS NOT NULL
    """)

    # Register transformed data as temp view
    df_transformed.createOrReplaceTempView(target_view)

    print(f"Transformed TEMP VIEW created: {target_view}")
    df_transformed.show(truncate=False)

    return df_transformed
