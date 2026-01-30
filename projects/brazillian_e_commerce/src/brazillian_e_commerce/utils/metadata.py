from pyspark.sql.functions import current_timestamp, lit

"""
Utility for adding ingestion metadata columns.
"""

def add_ingestion_metadata(df, source_system: str):
    
    """
    Adds standard ingestion metadata columns.

    Columns added:
        - ingestion_ts
        - source_system

    Args:
        df (DataFrame): Input dataframe
        source_system (str): Source system name

    Returns:
        DataFrame: DataFrame with metadata columns
    """


    return (
        df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit(source_system))
    )
