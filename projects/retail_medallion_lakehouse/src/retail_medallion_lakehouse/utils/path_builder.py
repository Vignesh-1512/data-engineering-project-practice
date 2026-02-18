def build_path(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"
