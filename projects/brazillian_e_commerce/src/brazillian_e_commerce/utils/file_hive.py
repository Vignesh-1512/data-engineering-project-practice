"""
Utility module for writing Spark DataFrames to Hive/Unity Catalog tables.
Spark Connect + Unity Catalog safe implementation.
"""

def write_table(
    df,
    target_table: str,
    mode: str,
    format: str = "delta"
):
    spark = df.sparkSession

    # -----------------------------
    # APPEND  → standard save
    # -----------------------------
    if mode == "append":
        (
            df.write
              .format(format)
              .mode("append")
              .option("mergeSchema", "true")
              .saveAsTable(target_table)
        )

    # -----------------------------
    # OVERWRITE → safe replace
    # -----------------------------
    elif mode == "overwrite":
        df.createOrReplaceTempView("_tmp_write")

        spark.sql(f"""
            CREATE OR REPLACE TABLE {target_table}
            USING {format}
            AS SELECT * FROM _tmp_write
        """)

    else:
        raise ValueError(f"Unsupported mode: {mode}")
