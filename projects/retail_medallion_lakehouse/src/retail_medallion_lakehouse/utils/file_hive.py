def write_csv(df, path, mode="overwrite"):
    (
        df.write
        .mode(mode)
        .option("header", "true")
        .csv(path)
    )


def write_parquet(df, path, mode="overwrite", partition_cols=None):
    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)
