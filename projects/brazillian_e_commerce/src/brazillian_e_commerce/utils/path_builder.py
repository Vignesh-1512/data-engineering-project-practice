def build_fqn(catalog: str, schema: str, table: str) -> str:
    """
    Builds fully qualified table name.
    Example:
    brazillian_e_commerce.bronze.bronze_orders
    """
    return f"{catalog}.{schema}.{table}"
