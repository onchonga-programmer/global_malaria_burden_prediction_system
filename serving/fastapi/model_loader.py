import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import mlflow.lightgbm
import os
import logging

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

WHO_REGIONS = ["AFRO", "AMRO", "EMRO", "EURO", "SEARO", "WPRO"]

# Map region to experiment name — exactly as they appear in your MLflow


PROPHET_EXPERIMENT_IDS = {
    "AFRO":  "2",
    "AMRO":  "6",
    "EMRO":  "4",
    "SEARO": "3",
    "WPRO":  "5",
}

LIGHTGBM_EXPERIMENT = "malaria_lightgbm"


def load_all_models():
    """
    Load all models from MLflow at startup.
    Searches by experiment name, takes the most recent run.
    Returns a dict with all models ready to use.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()
    models = {}

    # ── Load LightGBM ──────────────────────────────────────────────
    try:
        runs = client.search_runs(
            experiment_names=[LIGHTGBM_EXPERIMENT],
            order_by=["start_time DESC"],
            max_results=1
        )
        if runs:
            run_id = runs[0].info.run_id
            model_uri = f"runs:/{run_id}/model"
            models["lightgbm"] = mlflow.lightgbm.load_model(model_uri)
            models["lightgbm_run_id"] = run_id
            logger.info(f"LightGBM loaded from run {run_id}")
        else:
            logger.warning("No runs found in experiment: malaria_lightgbm")
    except Exception as e:
        logger.error(f"Failed to load LightGBM: {e}")

    # ── Load Prophet models per region ─────────────────────────────
    for region, experiment_id in PROPHET_EXPERIMENT_IDS.items():
        try:
            runs = client.search_runs(
                experiment_ids=[experiment_id],
                order_by=["start_time DESC"],
                max_results=1
            )
            if runs:
                run_id = runs[0].info.run_id
                model_uri = f"runs:/{run_id}/model"
                models[f"prophet_{region}"] = mlflow.pyfunc.load_model(model_uri)
                models[f"prophet_{region}_run_id"] = run_id
                logger.info(f"Prophet loaded for {region} — run {run_id}")
            else:
                logger.warning(f"No runs found in experiment: {experiment_id}")
        except Exception as e:
            logger.error(f"Failed to load Prophet for {region}: {e}")

    return models