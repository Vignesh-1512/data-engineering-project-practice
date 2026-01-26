from brazillian_e_commerce.utils.read import read_table
from brazillian_e_commerce.utils.metadata import add_ingestion_metadata
from brazillian_e_commerce.utils.write import write_table


def ingest_to_bronze(
    spark,
    source_table: str,
    target_table: str,
    load_type: str = "full",
    watermark_col: str | None = None,
    source_system: str = "olist_kaggle"
):
    """
    Generic Bronze ingestion function.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    source_table : str
        Fully qualified source table (e.g. olist.source.orders)
    target_table : str
        Fully qualified bronze table (e.g. olist.bronze.orders)
    load_type : str
        'full' or 'incremental'
    watermark_col : str | None
        Column used for incremental loading
    source_system : str
        Identifier for data source
    """

    # 1. Read source data (full or incremental)
    df = read_table(    
        spark=spark,
        source_table=source_table,
        load_type=load_type,
        watermark_col=watermark_col,
        target_table=target_table
    )

    # 2. Add standard Bronze metadata
    df = add_ingestion_metadata(df, source_system)

    # 3. Decide write mode
    write_mode = "overwrite" if load_type == "full" else "append"

    # 4. Write to Bronze table
    write_table(
        df=df,
        target_table=target_table,
        mode=write_mode
    )
