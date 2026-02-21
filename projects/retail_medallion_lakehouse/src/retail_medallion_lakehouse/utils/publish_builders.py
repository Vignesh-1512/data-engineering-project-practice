from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    sum as spark_sum,
    countDistinct,
    avg,
    count,
    when,
    to_date,
    monotonically_increasing_id
)


# ==========================================================
# 🔹 DIMENSION BUILDER
# ==========================================================
def build_dimension(df, config):

    dim_df = (
        df.select(*config["columns"])
          .dropDuplicates([config["natural_key"]])
          .withColumn(
              config["surrogate_key"],
              monotonically_increasing_id()
          )
          .withColumn("ingest_ts", current_timestamp())
    )

    return dim_df


# ==========================================================
# 🔹 FACT BUILDER
# ==========================================================
def build_fact(df, config):

    fact_df = df.select(*config["columns"])
    partition_by = []

    if config.get("partition_column") and config.get("partition_type") == "year":
        fact_df = fact_df.withColumn(
            "partition_year",
            year(col(config["partition_column"]))
        )
        partition_by = ["partition_year"]

    fact_df = fact_df.withColumn("ingest_ts", current_timestamp())

    return fact_df, partition_by


# ==========================================================
# 🔹 AGGREGATE BUILDER
# ==========================================================
def build_aggregate(df, config):

    group_cols = config["group_by"]

    # Handle date truncation
    if config.get("date_trunc") == "day":
        df = df.withColumn("sale_date", to_date(col(group_cols[0])))
        group_cols = ["sale_date"]

    grouped = df.groupBy(*group_cols)
    agg_exprs = []

    for alias, expr in config["metrics"].items():

        if expr.startswith("sum("):
            column = expr[4:-1]
            agg_exprs.append(spark_sum(column).alias(alias))

        elif expr.startswith("count_distinct("):
            column = expr[15:-1]
            agg_exprs.append(countDistinct(column).alias(alias))

        elif expr.startswith("avg("):
            column = expr[4:-1]
            agg_exprs.append(avg(column).alias(alias))

        elif expr.startswith("count_if("):
            condition = expr[9:-1]  # e.g. payment_status == SUCCESS
            left, op, right = condition.split()

            agg_exprs.append(
                count(
                    when(col(left) == right, True)
                ).alias(alias)
            )

    return grouped.agg(*agg_exprs)
