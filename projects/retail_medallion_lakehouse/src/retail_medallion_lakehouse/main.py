from retail_medallion_lakehouse.pre_landing.pre_landing import run_pre_landing
from retail_medallion_lakehouse.landing.run_landing import run_landing
from retail_medallion_lakehouse.unification.run_unification import run_unification
from retail_medallion_lakehouse.refinement.run_refinement import run_refinement
from retail_medallion_lakehouse.publish.run_publish import run_publish

def run(layer: str, 
        dataset: str = None,
        folder_type: str = None,
        start_year: int = None,
        end_year: int = None):
    """
    Main orchestration function.
    Routes execution based on layer.
    """

    print("\n===================================")
    print(f"🚀 PIPELINE STARTED | Layer: {layer}")
    print("===================================")

    try:
        if layer == "pre_landing":
            return run_pre_landing(layer_name=layer, 
                                    dataset_name=dataset,
                                    folder_type=folder_type,
                                    start_year=start_year,
                                    end_year=end_year)

        elif layer == "landing":
            return run_landing(layer_name=layer, dataset_name=dataset)

        elif layer == "unification":
            return run_unification(layer_name=layer, dataset_name=dataset)
        
        elif layer == "refinement":
            return run_refinement(layer_name=layer,dataset_name=dataset)
        elif layer == "publish":
            return run_publish(layer_name=layer,dataset_name=dataset)
        else:
            raise ValueError(f"Unsupported layer: {layer}")

    except Exception as e:
        print("\n❌ Pipeline Failed")
        print("Error Type:", type(e).__name__)
        print("Error Message:", str(e))
        raise

    finally:
        print("\n🏁 PIPELINE FINISHED\n")
