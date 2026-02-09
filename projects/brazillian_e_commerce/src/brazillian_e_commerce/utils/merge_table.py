from delta.tables import DeltaTable


def merge_upsert(
    spark,
    df,
    target_table: str,
    merge_key: str
):
    """
    Performs UPSERT using Delta merge.
    Used only in Silver layer.
    """

    # first load → create table
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    delta = DeltaTable.forName(spark, target_table)

    (
        delta.alias("t")
        .merge(
            df.alias("s"),
            f"t.{merge_key} = s.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
