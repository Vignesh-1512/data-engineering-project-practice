from brazillian_e_commerce.utils.fact_builder import build_fact
from brazillian_e_commerce.utils.dim_builder import build_dim
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark
from brazillian_e_commerce.utils.exceptions import ConfigError, TransformationError
from brazillian_e_commerce.utils.path_builder import build_fqn


def run_model(
    layer: str, 
    table_name: str | None = None,
    mode: str ="overwrite"    
):


    """
    Runs the Gold modeling layer.

    Builds fact and dimension tables dynamically using
    configured source tables and generic builders.

    Args:
        layer (str): Pipeline layer name (gold)
        table_name (str | None): Specific fact/dim table or all
        mode (str): Write mode for gold tables

    Raises:
        ConfigError: If gold config is invalid
        TransformationError: If fact/dim build fails
        DataWriteError: If write to gold fails
    """

    try:
        spark = get_spark()
        config = load_config("tables.yaml")[layer]

        print("\n[GOLD MODELING STARTED]")

    except Exception as e:
        raise ConfigError(f"Failed to load {layer} config. Reason: {str(e)}")

    tables = {table_name: config[table_name]} if table_name else config

    for name, cfg in tables.items():

        print(f"\n→ Building GOLD table : {name}")
        print(f"  Type              : {cfg['type']}")
        print(f"  Grain             : {cfg['grain']}")

        try:
            sources = {
                alias: spark.read.table(
                    build_fqn(
                        cfg["catalog"],
                        cfg["source_schema"],
                        table
                    )
                )
                for alias, table in cfg["source_tables"].items()
            }

            df = (
                build_fact(name, **sources)
                if cfg["type"] == "fact"
                else build_dim(name, **sources)
            )

            print(f"  Output rows       : {df.count()}")
        except Exception as e:
            raise TransformationError(
                f"[Gold] Failed building '{name}'. Reason: {str(e)}"
            )

        target_fqn = build_fqn(
            cfg["catalog"],
            cfg["target_schema"],
            cfg["table_name"]
        )

        write_table(
            df=df,
            target_table=target_fqn,
            mode=mode,
            overwrite_schema=True
        )
        print(f"  ✅ Gold table created : {target_fqn}")

