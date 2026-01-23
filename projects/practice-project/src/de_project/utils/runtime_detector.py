import os

def get_runtime_mode() -> str:
    """
    Detect execution environment automatically.
    Returns: 'databricks' or 'local'
    """
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return "databricks"
    return "local"
