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
        dataset_name: str | None = None
):
    """
    Pre-Landing ingestion.

    - Reads JSON from API
    - Applies column translation (German → English)
    - Explodes nested items
    - Extracts contacts
    - Adds ingestion timestamp
    - Writes to Unity Catalog table

    Supports:
    - Single dataset run
    - All datasets run
    """

    spark = get_spark(app_name=f"retail_{layer_name}")

    try:
        layer_config = load_config()[layer_name]
    except Exception as error:
        raise Exception(
            f"Failed to load config for layer '{layer_name}'. "
            f"Reason: {str(error)}"
        )

    print("\n[PRE-LANDING STARTED]")

    # -------------------------------------------------------
    # Determine which datasets to process
    # -------------------------------------------------------
    if dataset_name:
        datasets_to_process = {dataset_name: layer_config[dataset_name]}
    else:
        datasets_to_process = {
            name: config
            for name, config in layer_config.items()
            if name != "write_mode"
        }


    # -------------------------------------------------------
    # Process each dataset
    # -------------------------------------------------------
    for dataset, dataset_config in datasets_to_process.items():

        try:
            print(f"\n→ Processing dataset : {dataset}")

            write_mode = layer_config["write_mode"]

            # -----------------------------
            # READ SOURCE JSON
            # -----------------------------
            dataframe = read_data(
                spark=spark,
                path=dataset_config["input_path"],
                file_type=dataset_config["file_type"],
                schema=None
            )

            print(f"  Rows read : {dataframe.count()}")
            dataframe.printSchema()
            dataframe.show(5, truncate=False)

            # -----------------------------
            # COLUMN TRANSLATION
            # -----------------------------
            translation_config = dataset_config.get("translation")

            if translation_config:

                print("  Applying column translation...")

                translation_table_name = build_path(
                    translation_config["catalog"],
                    translation_config["schema"],
                    translation_config["table_name"]
                )

                translation_dataframe = spark.table(
                    translation_table_name
                )

                column_mapping = {
                    row[translation_config["source_column"]]:
                    row[translation_config["target_column"]]
                    for row in translation_dataframe.collect()
                }

                # Root level rename
                for column_name in dataframe.columns:
                    if column_name in column_mapping:
                        dataframe = dataframe.withColumnRenamed(
                            column_name,
                            column_mapping[column_name]
                        )

                # Nested rename inside 'items'
                if "items" in dataframe.columns:

                    # Get items struct fields from schema (outside column)
                    item_fields=dataframe.schema["items"].dataType.elementType.fieldNames()

                    dataframe = dataframe.withColumn(
                        "items",
                        transform(
                            col("items"),
                            lambda nested_row: struct(
                                *[
                                    nested_row[field_name].alias(
                                        column_mapping.get(field_name, field_name)
                                    )
                                    for field_name in item_fields
                                ]
                            )
                        )
                    )

            # -----------------------------
            # EXPLODE ITEMS
            # -----------------------------
            if "items" in dataset_config.get("explode_columns", []):

                dataframe = dataframe.withColumn(
                    "item",
                    explode(col("items"))
                )

            # -----------------------------
            # EXTRACT CONTACT DETAILS
            # -----------------------------
            contacts_config = dataset_config.get("contacts")

            if contacts_config:

                print("  Extracting contact details...")

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


            # -----------------------------
            # SELECT FINAL COLUMNS
            # -----------------------------
            selected_columns = []

            existing_columns = dataframe.columns

            for column_expression in dataset_config["select_columns"]:
                if " as " in column_expression:
                    original_column, alias_name = column_expression.split(" as ")
                else:
                    original_column = column_expression
                    alias_name = None

                # Split first level
                root_column = original_column.split(".")[0]

                if original_column in existing_columns:
                    # Flat column like "customer.cust_id"
                    column_obj = col(f"`{original_column}`")
                elif root_column in existing_columns:
                    # Struct navigation like "item.product_id"
                    column_obj = col(original_column)
                else:
                    column_obj = col(original_column)

                if alias_name:
                    column_obj = column_obj.alias(alias_name)

                selected_columns.append(column_obj)




            if contacts_config:
                for output_column in contacts_config["mappings"].values():
                    selected_columns.append(col(output_column))

            dataframe = dataframe.select(*selected_columns)

            # -----------------------------
            # ADD INGESTION TIMESTAMP
            # -----------------------------
            dataframe = dataframe.withColumn(
                "ingest_ts",
                current_timestamp()
            )

            # -----------------------------
            # BUILD TARGET TABLE NAME
            # -----------------------------
            target_table_name = build_path(
                dataset_config["catalog"],
                dataset_config["target_schema"],
                dataset_config["table_name"]
            )

            # -----------------------------
            # WRITE TO UNITY CATALOG
            # -----------------------------
            write_table(
                df=dataframe,
                target_table=target_table_name,
                mode=write_mode
            )

            print(f"  Pre-landing completed for {dataset}")

        except Exception as error:
            raise Exception(
                f"[Pre-Landing] Failed for dataset '{dataset}'. "
                f"Reason: {str(error)}"
            )

    print("\n[PRE-LANDING COMPLETED]")
