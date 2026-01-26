def drop_nulls(df, columns: list):
    """
    Drop rows where any of the given columns are NULL.
    """
    for col_name in columns:
        df = df.filter(f"{col_name} IS NOT NULL")
    return df
