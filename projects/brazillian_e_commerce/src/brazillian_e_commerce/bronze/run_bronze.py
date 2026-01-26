from brazillian_e_commerce.utils.spark import get_spark
from brazillian_e_commerce.utils.config import load_config
from brazillian_e_commerce.bronze.ingest import ingest_to_bronze


def run_bronze(table_name: str | None = None):
    """
    Bronze layer orchestrator.
    """
    spark = get_spark()
    config = load_config("tables.yaml")["bronze"]

    if table_name:
        cfg = config[table_name]

        ingest_to_bronze(
            spark=spark,
            source_table=cfg["source_table"],
            target_table=cfg["target_table"],
            load_type=cfg.get("load_type", "full"),
            watermark_col=cfg.get("watermark_column")
        )

    else:
        for cfg in config.values():
            ingest_to_bronze(
                spark=spark,
                source_table=cfg["source_table"],
                target_table=cfg["target_table"],
                load_type=cfg.get("load_type", "full"),
                watermark_col=cfg.get("watermark_column")
            )
