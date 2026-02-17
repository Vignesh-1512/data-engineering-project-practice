import requests
import json

from retail_medallion_lakehouse.utils.schema_builder import build_schema

def read_data(spark, path: str, file_type: str, schema:None):

    file_type = file_type.lower()

    if file_type == "json" and path.startswith("http"):

        response = requests.get(path)

        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code}")

        data = response.json()

        if isinstance(data, dict):
            data = [data]

        if schema:
            return spark.createDataFrame(data, schema=schema)

        return spark.createDataFrame(data)




    elif file_type == "csv":
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )

    elif file_type == "parquet":
        return spark.read.parquet(path)

    else:
        raise ValueError(f"Unsupported file type: {file_type}")
