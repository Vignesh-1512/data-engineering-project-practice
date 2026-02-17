from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType


def basic_trim(df):
    """
    Trims whitespace from all string columns.
    """

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    return df
