from pyspark.sql.functions import (
    col,
    explode,
    expr,
    current_timestamp,
    transform,
    struct
)
from pyspark.sql.functions import element_at

from retail_medallion_lakehouse.utils.spark import get_spark
from retail_medallion_lakehouse.utils.config_loader import load_config
from retail_medallion_lakehouse.utils.file_read import read_data
from retail_medallion_lakehouse.utils.file_hive import write_table
from retail_medallion_lakehouse.utils.path_builder import build_path


def run_pre_landing(
        layer_name: str,
        dataset_name: str | None = None,
        folder_type: str | None = None,
        start_year: int | None = None,
        end_year: int | None = None
):

    spark = get_spark(app_name=f"retail_{layer_name}")

    try:
        layer_config = load_config()[layer_name]
    except Exception as error:
        raise Exception(
            f"Failed to load config for layer '{layer_name}'. "
            f"Reason: {str(error)}"
        )

    print("\n==============================")
    print("🚀 PRE-LANDING STARTED")
    print("==============================")

    # ------------------------------
    # Validate Inputs
    # ------------------------------
    if not folder_type:
        raise Exception("folder_type must be provided.")

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            raise Exception("start_year cannot be greater than end_year.")

    # ------------------------------
    # Determine datasets
    # ------------------------------
    if dataset_name:
        datasets_to_process = {dataset_name: layer_config[dataset_name]}
    else:
        datasets_to_process = {
            name: config
            for name, config in layer_config.items()
            if name != "write_mode"
        }

    # ------------------------------
    # Process each dataset
    # ------------------------------
    for dataset, dataset_config in datasets_to_process.items():

        try:
            print(f"\n→ Processing dataset: {dataset}")

            write_mode = layer_config["write_mode"]
            base_url = dataset_config["input_base_path"]
            file_type = dataset_config["file_type"]

            dataframes = []

            # ------------------------------
            # RANGE LOAD
            # ------------------------------
            if start_year is not None and end_year is not None:

                print(f"  Loading range {start_year} → {end_year}")

                for year in range(start_year, end_year + 1):

                    file_url = f"{base_url}/{folder_type}/{year}_{dataset}.json"
                    print(f"  Reading: {file_url}")

                    df = read_data(
                        spark=spark,
                        path=file_url,
                        file_type=file_type,
                        schema=None
                    )

                    dataframes.append(df)

            # ------------------------------
            # FULL FOLDER LOAD
            # ------------------------------
            else:

                print("  No year range provided → attempting full folder load.")

                # Safe year window
                for year in range(2000, 2035):

                    file_url = f"{base_url}/{folder_type}/{year}_{dataset}.json"

                    try:
                        df = read_data(
                            spark=spark,
                            path=file_url,
                            file_type=file_type,
                            schema=None
                        )
                        dataframes.append(df)
                        print(f"  Loaded: {file_url}")
                    except:
                        continue

            if not dataframes:
                raise Exception(f"No data files found for dataset '{dataset}'.")

            # ------------------------------
            # UNION ALL YEARS
            # ------------------------------
            dataframe = dataframes[0]

            for df in dataframes[1:]:
                dataframe = dataframe.unionByName(df, allowMissingColumns=True)

            print(f"  Total Rows Loaded: {dataframe.count()}")

            # ------------------------------
            # COLUMN TRANSLATION
            # ------------------------------
            translation_config = dataset_config.get("translation")

            if translation_config:

                translation_table_name = build_path(
                    translation_config["catalog"],
                    translation_config["schema"],
                    translation_config["table_name"]
                )

                translation_dataframe = spark.table(translation_table_name)

                column_mapping = {
                    row[translation_config["source_column"]]:
                    row[translation_config["target_column"]]
                    for row in translation_dataframe.collect()
                }

                # Root rename
                for column_name in dataframe.columns:
                    if column_name in column_mapping:
                        dataframe = dataframe.withColumnRenamed(
                            column_name,
                            column_mapping[column_name]
                        )

                # Nested rename inside items
                if "items" in dataframe.columns:
                    item_fields = dataframe.schema["items"].dataType.elementType.fieldNames()

                    dataframe = dataframe.withColumn(
                        "items",
                        transform(
                            col("items"),
                            lambda nested_row: struct(
                                *[
                                    nested_row[field].alias(
                                        column_mapping.get(field, field)
                                    )
                                    for field in item_fields
                                ]
                            )
                        )
                    )

            # ------------------------------
            # EXPLODE ITEMS
            # ------------------------------
            if "items" in dataset_config.get("explode_columns", []):
                dataframe = dataframe.withColumn(
                    "item",
                    explode(col("items"))
                )

            # ------------------------------
            # CONTACT EXTRACTION
            # ------------------------------
            contacts_config = dataset_config.get("contacts")

            if contacts_config:

                parent_column = contacts_config["parent"]
                type_field = contacts_config["type_field"]
                value_field = contacts_config["value_field"]

                for contact_type, output_column in contacts_config["mappings"].items():
                    dataframe = dataframe.withColumn(
                        output_column,
                        element_at(
                            expr(
                                f"filter(`{parent_column}`, x -> x.{type_field} = '{contact_type}')"
                            ),
                            1
                        )[value_field]
                    )

            # ------------------------------
            # SELECT FINAL COLUMNS
            # ------------------------------
            selected_columns = []

            for column_expression in dataset_config["select_columns"]:

                if " as " in column_expression:
                    original_column, alias_name = column_expression.split(" as ")
                else:
                    original_column = column_expression
                    alias_name = None

                column_obj = col(original_column)

                if alias_name:
                    column_obj = column_obj.alias(alias_name)

                selected_columns.append(column_obj)

            dataframe = dataframe.select(*selected_columns)

            # ------------------------------
            # ADD INGEST TIMESTAMP
            # ------------------------------
            dataframe = dataframe.withColumn(
                "ingest_ts",
                current_timestamp()
            )

            # ------------------------------
            # WRITE TO UNITY CATALOG
            # ------------------------------
            target_table_name = build_path(
                dataset_config["catalog"],
                dataset_config["target_schema"],
                dataset_config["table_name"]
            )

            write_table(
                df=dataframe,
                target_table=target_table_name,
                mode=write_mode
            )

            print(f"  ✅ Completed dataset: {dataset}")

        except Exception as error:
            raise Exception(
                f"[Pre-Landing] Failed for dataset '{dataset}'. "
                f"Reason: {str(error)}"
            )

    print("\n🏁 PRE-LANDING COMPLETED")