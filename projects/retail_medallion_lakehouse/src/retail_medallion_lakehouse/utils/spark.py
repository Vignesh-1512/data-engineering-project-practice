from pyspark.sql import SparkSession


def get_spark(app_name: str, master: str = None) -> SparkSession:

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    spark = (
        builder
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

    return spark
    