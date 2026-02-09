from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


def input_data_generator(spark, table_name: str, schema):

    if table_name != "orders":
        return spark.createDataFrame([], schema)

    rows = [
        (
            "e481f51cbdc54678b7cc49136f2d6af7",  # same id → update test
            "9ef432eb6251297304e76186b10a928d",
            "shipped",  # changed status (simulate update)
            datetime(2017, 10, 2, 10, 56),
            datetime(2017, 10, 2, 11, 7),
            datetime(2017, 10, 4, 19, 55),
            datetime(2017, 10, 10, 21, 25),
            datetime(2017, 10, 18, 0, 0)
        ),
        (
            "new_test_order_001",  # new id → insert test
            "9ef432eb6251297304e76186b10a928d",
            "processing",
            datetime(2017, 10, 2, 10, 56),
            datetime(2017, 10, 2, 11, 7),
            datetime(2017, 10, 4, 19, 55),
            datetime(2017, 10, 10, 21, 25),
            datetime(2017, 10, 18, 0, 0)
        )
    ]

    return spark.createDataFrame(rows, schema)
