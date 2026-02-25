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
    min as spark_min,
    max as spark_max,
    round,
    to_json,
    array,
    struct,
    expr,
    lit
)


# ==========================================================
# 🔹 DIMENSION BUILDER
# ==========================================================
def build_dimension(df, config):

    dim_df = (
        df.select(*config["columns"])
          .dropDuplicates([config["natural_key"]])
    )

    # Special handling for customer contacts
    if config["target_table"] == "customer_dimension":

        dim_df = dim_df.withColumn(
            "customer_contacts",
            to_json(
                array(
                    struct(
                        lit("email").alias("type"),
                        col("customer_email").alias("value")
                    ),
                    struct(
                        lit("phone").alias("type"),
                        col("customer_phone").alias("value")
                    )
                )
            )
        ).drop("customer_email", "customer_phone")

    dim_df = dim_df.withColumn("ingest_ts", current_timestamp())

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
        df = df.withColumn("txn_date", to_date(col(group_cols[0])))
        group_cols = ["txn_date"]

    grouped = df.groupBy(*group_cols)
    agg_exprs = []

    for alias, metric_expr in config["metrics"].items():

        if metric_expr.startswith("sum("):
            column = metric_expr[4:-1]
            agg_exprs.append(spark_sum(column).alias(alias))

        elif metric_expr.startswith("count_distinct("):
            column = metric_expr[15:-1]
            agg_exprs.append(countDistinct(column).alias(alias))

        elif metric_expr.startswith("avg("):
            column = metric_expr[4:-1]
            agg_exprs.append(avg(column).alias(alias))

        elif metric_expr.startswith("min("):
            column = metric_expr[4:-1]
            agg_exprs.append(spark_min(column).alias(alias))

        elif metric_expr.startswith("max("):
            column = metric_expr[4:-1]
            agg_exprs.append(spark_max(column).alias(alias))

        elif metric_expr.startswith("count_if("):
            condition = metric_expr[9:-1]
            left, op, right = condition.split()

            right= right.strip("'")
            
            if op == "==":
                agg_exprs.append(
                    count(when(col(left) == right, True)).alias(alias)
                )
            elif op == "!=":
                agg_exprs.append(
                    count(when(col(left) != right, True)).alias(alias)
                )

    result = grouped.agg(*agg_exprs)

    # Add payment success percentages
    if config["target_table"] == "payment_success_rate":

        result = result.withColumn(
            "success_rate_pct",
            round(expr("try_divide(captured,attempts)*100"), 2)
        ).withColumn(
            "failure_rate_pct",
            round(expr("try_divide(failed,attempts)*100"), 2)
        )

    return result