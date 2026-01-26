from pyspark.sql.functions import current_timestamp, lit


def add_ingestion_metadata(df, source_system: str):
    """
    Adds standard ingestion metadata columns.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame
    source_system : str
        Source system name (e.g. olist_kaggle)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with metadata columns added
    """

    return (
        df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit(source_system))
    )
