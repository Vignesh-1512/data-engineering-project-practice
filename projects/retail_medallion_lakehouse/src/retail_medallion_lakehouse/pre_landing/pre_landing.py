from pyspark.sql.functions import col, explode, expr
from retail_medallion_lakehouse.utils.spark import get_spark
from retail_medallion_lakehouse.utils.file_read import read_data
from retail_medallion_lakehouse.utils.cleaner import basic_trim
from retail_medallion_lakehouse.utils.config_loader import load_config
from retail_medallion_lakehouse.utils.schema_builder import build_schema
from retail_medallion_lakehouse.utils.exceptions import LayerNotFoundException

def run_pre_landing(layer: str, dataset_name: str = None):
    """
    Executes Pre-Landing layer.
    
    Supports:
    - Single dataset execution
    - All datasets execution (if dataset_name is None)
    """

    print("\n==============================")
    print(f"🚀 Starting Layer: {layer}")
    print("==============================")

    try:
        # --------------------------------
        # 1️⃣ Load Full Config
        # --------------------------------
        full_config = load_config()

        if layer not in full_config:
            raise LayerNotFoundException(f"❌ Layer '{layer}' not found in configuration.")

        layer_config = full_config[layer]

        # --------------------------------
        # 2️⃣ Initialize Spark
        # --------------------------------
        spark = get_spark("PreLandingLayer")
        print("✅ Spark Session Initialized")

        # --------------------------------
        # 3️⃣ Run Single Dataset
        # --------------------------------
        if dataset_name:

            if dataset_name not in layer_config:
                raise ValueError(f"❌ Dataset '{dataset_name}' not found under layer '{layer}'.")

            return _process_dataset(spark, dataset_name, layer_config[dataset_name])

        # --------------------------------
        # 4️⃣ Run All Datasets
        # --------------------------------
        else:
            print("🔄 No dataset specified → Running ALL datasets")
            results = {}

            for ds_name, ds_config in layer_config.items():
                print(f"\n-----------------------------------")
                print(f"📦 Processing Dataset: {ds_name}")
                print("-----------------------------------")

                results[ds_name] = _process_dataset(spark, ds_name, ds_config)

            return results

    except Exception as e:
        print("\n❌ ERROR in Pre-Landing Layer")
        print("Error Type:", type(e).__name__)
        print("Error Message:", str(e))
        raise

    finally:
        print("\n🏁 Pre-Landing Execution Completed\n")


# ======================================================
# 🔹 Internal Dataset Processor
# ======================================================

def _process_dataset(spark, dataset_name: str, config: dict):

    try:
        print(f"📥 Reading dataset: {dataset_name}")

        # --------------------------------
        # 1️⃣ Schema Handling
        # --------------------------------
        schema = None

        if "schema" in config:
            print("🔧 Building schema from YAML...")
            schema = build_schema(config["schema"])

        # --------------------------------
        # 2️⃣ Read Data
        # --------------------------------
        df = read_data(
            spark,
            config["input_path"],
            config["file_type"],
            schema
        )

        print("✅ Data Read Successfully")
        print("Initial Row Count:", df.count())

        # --------------------------------
        # 3️⃣ Explode Configured Arrays
        # --------------------------------
        for column in config.get("explode_columns", []):
            if column in df.columns:
                print(f"🔄 Exploding column: {column}")
                df = df.withColumn("item", explode(col(column)))

        # --------------------------------
        # 4️⃣ Extract Contacts
        # --------------------------------
        contacts_cfg = config.get("contacts")

        if contacts_cfg:
            print("📞 Extracting Contact Fields")

            parent = contacts_cfg["parent"]
            type_field = contacts_cfg["type_field"]
            value_field = contacts_cfg["value_field"]

            for contact_type, output_col in contacts_cfg["mappings"].items():
                print(f"   ➜ Extracting {contact_type} → {output_col}")

                df = df.withColumn(
                    output_col,
                    expr(
                        f"filter({parent}, x -> x.{type_field} = '{contact_type}')[0].{value_field}"
                    )
                )

        # --------------------------------
        # 5️⃣ Dynamic Column Selection
        # --------------------------------
        print("📌 Selecting Required Columns")

        select_exprs = []

        for field in config["select_columns"]:
            if " as " in field:
                original, alias = field.split(" as ")
                print(f"   ➜ Renaming {original} → {alias}")
                select_exprs.append(col(original).alias(alias))
            else:
                select_exprs.append(col(field))

        if contacts_cfg:
            for output_col in contacts_cfg["mappings"].values():
                select_exprs.append(col(output_col))

        df = df.select(*select_exprs)

        # --------------------------------
        # 6️⃣ Basic Cleaning
        # --------------------------------
        print("🧹 Applying Basic Trim Cleaning")
        df = basic_trim(df)

        # --------------------------------
        # 7️⃣ Stats & Preview
        # --------------------------------
        print("\n📊 Dataset Statistics")
        total_rows = df.count()
        distinct_rows = df.distinct().count()

        print("Total Rows   :", total_rows)
        print("Distinct Rows:", distinct_rows)

        if total_rows != distinct_rows:
            print("⚠ Duplicate records detected!")

        print("\n🔎 Data Preview:")
        df.show(5, truncate=False)
        df.printSchema()

        return df

    except Exception as e:
        print(f"\n❌ ERROR while processing dataset '{dataset_name}'")
        print("Error Type:", type(e).__name__)
        print("Error Message:", str(e))
        raise
