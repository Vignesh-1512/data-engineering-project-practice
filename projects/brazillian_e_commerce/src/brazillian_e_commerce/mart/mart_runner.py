from brazillian_e_commerce.utils.config_loader import load_config
from brazillian_e_commerce.utils.spark_session import get_spark
from brazillian_e_commerce.utils.file_hive import write_table
from brazillian_e_commerce.mart.builders import BUILDERS
from brazillian_e_commerce.utils.exceptions import TransformationError
from brazillian_e_commerce.utils.path_builder import build_fqn


def run_mart(
    layer: str, 
    br_name: str | None = None,
    mode: str ="overwrite"
):

    """
    Runs the Mart (Final) layer for business requirements.

    Executes analytical transformations for each BR
    using gold tables and writes curated outputs.

    Args:
        layer (str): Pipeline layer name (mart)
        br_name (str | None): Specific business requirement or all
        mode (str): Write mode for final tables

    Raises:
        ConfigError: If mart config fails
        TransformationError: If BR logic fails
        DataWriteError: If final write fails
    """

    spark = get_spark()
    config = load_config("tables.yaml")[layer]

    print("\n[MART EXECUTION STARTED]")

    if br_name:
        _run_single_table(spark, br_name, config[br_name],mode)
    else:
        for name, cfg in config.items():
            _run_single_table(spark, name, cfg,mode)


def _run_single_table(spark, name, cfg,mode):
    try:
        tables = {
            alias: spark.read.table(
                build_fqn(
                    cfg["catalog"],
                    cfg["source_schema"],
                    table
                )
            )
            for alias, table in cfg["source_tables"].items()
        }

        df = BUILDERS[name](**tables)

    except Exception as e:
        raise TransformationError(
            f"[Mart] Failed BR '{name}'. Reason: {str(e)}"
        )

    target_fqn = build_fqn(
        cfg["catalog"],
        cfg["target_schema"],
        cfg["table_name"]
    )

    write_table(df, target_fqn, mode=mode)
    print(f"  ✅ BR completed : {target_fqn}")
