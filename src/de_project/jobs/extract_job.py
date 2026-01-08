import logging
from pyspark.sql import SparkSession
from de_project.utils.config_loader import load_config

logger = logging.getLogger(__name__)

def extract_run(dataset: str, process_date: str):
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    
    runtime = get_runtime_mode()
    print(f"Running in {runtime.upper()} mode")

    dataset_config = config["datasets"][dataset][runtime]
    csv_dir = dataset_config["csv_path"]

    safe_date = process_date.replace("-", "_")
    csv_file = f"{csv_dir}/{dataset}_{safe_date}.csv"
    temp_view = f"{dataset}_{safe_date}"

    print(f"CSV FILE PATH = {csv_file}")

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(csv_file)
        )

        df.createOrReplaceTempView(temp_view)
        print(f"CSV loaded as TEMP TABLE: {temp_view}")

        spark.sql(f"SELECT * FROM {temp_view}").show(truncate=False)
        return df

    except Exception as e:
        logger.error(f"CSV read failed: {e}")
        raise
