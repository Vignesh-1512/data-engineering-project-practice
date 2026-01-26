from pyspark.sql import SparkSession


def get_spark(app_name: str = "olist-data-engineering"):
    """
    Returns a SparkSession.
    Works both locally and in Databricks.
    """

    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
