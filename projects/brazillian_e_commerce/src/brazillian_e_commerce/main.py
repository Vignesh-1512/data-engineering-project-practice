from brazillian_e_commerce.bronze.ingest import run_ingest
from brazillian_e_commerce.silver.refine import run_refine
from brazillian_e_commerce.gold.model import run_model
from brazillian_e_commerce.mart.mart_runner import run_mart


def run(layer: str, 
        table_name: str | None = None,
        mode: str = "overwrite"
):  

    print("=" * 80)
    print(f"[PIPELINE STARTED]")
    print(f"Layer        : {layer}")
    print(f"Table/BR     : {table_name if table_name else 'ALL'}")
    print(f"Write mode   : {mode}")
    print("=" * 80)


    if layer == "bronze":
        run_ingest(layer,table_name,mode)
    elif layer == "silver":
        run_refine(layer,table_name,mode)
    elif layer == "gold":
        run_model(layer,table_name,mode)
    elif layer == "mart":
        run_mart(layer,table_name,mode)
    else:
        raise ValueError(f"Invalid layer : {layer}")
