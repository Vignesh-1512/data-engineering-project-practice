from delta.tables import DeltaTable


def merge_upsert(
    spark_session,
    source_dataframe,
    target_table_name: str,
    merge_keys
):
    """
    Perform UPSERT (MERGE) into Delta table.

    Parameters
    ----------
    spark_session : SparkSession
    source_dataframe : DataFrame
        New/changed records to merge
    target_table_name : str
        Fully qualified table name
    merge_keys : str | list[str]
        Primary key(s) for merge
        - single column  -> "order_id"
        - composite key  -> ["order_id", "order_item_id"]
    """


    # -------------------------------------------------
    # First load → create table
    # -------------------------------------------------
    if not spark_session.catalog.tableExists(target_table_name):
        (
            source_dataframe.write
            .format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .saveAsTable(target_table_name)
        )
        return

    delta_target_table = DeltaTable.forName(spark_session, target_table_name)

    # -------------------------------------------------
    # Build dynamic merge condition
    # -------------------------------------------------
    if isinstance(merge_keys, list):

        merge_condition = " AND ".join(
            [
                f"target.{column_name} = source.{column_name}"
                for column_name in merge_keys
            ]
        )

    else:
        merge_condition = f"target.{merge_keys} = source.{merge_keys}"

    # -------------------------------------------------
    # Execute MERGE (UPSERT)
    # -------------------------------------------------
    (
        delta_target_table.alias("target")
        .merge(
            source_dataframe.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
