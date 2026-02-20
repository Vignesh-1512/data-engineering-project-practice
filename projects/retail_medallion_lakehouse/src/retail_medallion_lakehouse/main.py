from retail_medallion_lakehouse.pre_landing.pre_landing import run_pre_landing
from retail_medallion_lakehouse.landing.run_landing import run_landing
from retail_medallion_lakehouse.unification.run_unification import run_unification


def run(layer: str, dataset: str = None):
    """
    Main orchestration function.
    Routes execution based on layer.
    """

    print("\n===================================")
    print(f"🚀 PIPELINE STARTED | Layer: {layer}")
    print("===================================")

    try:
        if layer == "pre_landing":
            return run_pre_landing(layer_name=layer, dataset_name=dataset)

        elif layer == "landing":
            return run_landing(layer_name=layer, dataset_name=dataset)

        elif layer == "unification":
            return run_unification(layer_name=layer, dataset_name=dataset)

        else:
            raise ValueError(f"Unsupported layer: {layer}")

    except Exception as e:
        print("\n❌ Pipeline Failed")
        print("Error Type:", type(e).__name__)
        print("Error Message:", str(e))
        raise

    finally:
        print("\n🏁 PIPELINE FINISHED\n")
