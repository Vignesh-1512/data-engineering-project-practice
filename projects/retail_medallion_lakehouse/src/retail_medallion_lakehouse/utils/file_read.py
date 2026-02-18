import requests
import pandas as pd


# ==========================================================
# READ FROM PATH (Pre-Landing)
# ==========================================================
def read_data(spark, path: str, file_type: str, schema=None):

    try:
        file_type = file_type.lower()

        # ---------------------------------------------------
        # JSON FROM API
        # ---------------------------------------------------
        if file_type == "json" and path.startswith("http"):

            response = requests.get(path, timeout=30)

            if response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code}")

            data = response.json()

            if isinstance(data, dict):
                data = [data]

            pdf = pd.json_normalize(data)

            if schema:
                return spark.createDataFrame(pdf, schema=schema)

            return spark.createDataFrame(pdf)

        # ---------------------------------------------------
        # JSON FROM FILE
        # ---------------------------------------------------
        elif file_type == "json":

            reader = spark.read.option("multiLine", "true")

            if schema:
                reader = reader.schema(schema)

            return reader.json(path)

        # ---------------------------------------------------
        # CSV
        # ---------------------------------------------------
        elif file_type == "csv":
            return (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
            )

        # ---------------------------------------------------
        # PARQUET
        # ---------------------------------------------------
        elif file_type == "parquet":
            return spark.read.parquet(path)

        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    except Exception as e:
        raise Exception(
            f"[READ DATA] Failed reading from path: {path}. "
            f"Reason: {str(e)}"
        )


# ==========================================================
# READ FROM UNITY CATALOG TABLE (Landing & Above)
# ==========================================================
def read_table(spark, source_table: str):
    """
    Reads a Unity Catalog managed table safely.
    Used in Landing / Silver / Gold layers.
    """

    try:
        print(f"\n📖 Reading Table: {source_table}")

        if not spark.catalog.tableExists(source_table):
            raise Exception(f"Table does not exist: {source_table}")

        df = spark.table(source_table)

        print(f"   Rows read: {df.count()}")

        return df

    except Exception as e:
        raise Exception(
            f"[READ_TABLE] Failed reading table: {source_table}. "
            f"Reason: {str(e)}"
        )
