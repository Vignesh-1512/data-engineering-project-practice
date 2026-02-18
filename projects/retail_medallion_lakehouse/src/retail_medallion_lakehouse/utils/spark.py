from pyspark.sql import SparkSession


def get_spark(app_name: str):
    """
    Returns active Spark session inside Databricks.
    Does NOT create a new Spark session.
    """

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    return spark
