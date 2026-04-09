import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import os
import logging

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
WHO_REGIONS = ["AFRO", "AMRO", "EMRO", "EURO", "SEARO", "WPRO"]

def load_all_models():
    """
    Load all models from MLflow at startup.
    Returns a dict with all models ready to use.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    models = {}

    # Load LightGBM classifier
    try:
        client = mlflow.tracking.MlflowClient()
        # Get the latest run with our classifier
        runs = client.search_runs(
            experiment_ids=["1"],  # adjust to your experiment ID
            filter_string="tags.model_type = 'lightgbm_classifier'",
            order_by=["start_time DESC"],
            max_results=1
        )
        if runs:
            run_id = runs[0].info.run_id
            model_uri = f"runs:/{run_id}/lightgbm_model"
            models["lightgbm"] = mlflow.sklearn.load_model(model_uri)
            models["lightgbm_run_id"] = run_id
            logger.info(f"LightGBM loaded from run {run_id}")
        else:
            logger.warning("No LightGBM model found in MLflow")
    except Exception as e:
        logger.error(f"Failed to load LightGBM: {e}")

    # Load Prophet models for each region
    for region in WHO_REGIONS:
        try:
            runs = client.search_runs(
                experiment_ids=["1"],
                filter_string=f"tags.model_type = 'prophet' AND tags.region = '{region}'",
                order_by=["start_time DESC"],
                max_results=1
            )
            if runs:
                run_id = runs[0].info.run_id
                model_uri = f"runs:/{run_id}/prophet_model"
                models[f"prophet_{region}"] = mlflow.pyfunc.load_model(model_uri)
                logger.info(f"Prophet loaded for {region} from run {run_id}")
            else:
                logger.warning(f"No Prophet model found for region {region}")
        except Exception as e:
            logger.error(f"Failed to load Prophet for {region}: {e}")

    return models
