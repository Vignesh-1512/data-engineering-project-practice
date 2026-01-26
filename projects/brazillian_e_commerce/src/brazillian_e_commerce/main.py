from brazillian_e_commerce.bronze.run_bronze import run_bronze
from brazillian_e_commerce.silver.run_silver import run_silver
from brazillian_e_commerce.gold.run_gold import run_gold
from brazillian_e_commerce.final.run_final import run_final


def run(layer: str, table_name: str | None = None):
    """
    Pipeline entry point.
    """

    if layer == "bronze":
        run_bronze(table_name)

    elif layer == "silver":
        run_silver(table_name)

    elif layer == "gold":
        run_gold(table_name)

    elif layer == "final":
        run_final(table_name)

    else:
        raise ValueError(f"Unsupported layer: {layer}")
