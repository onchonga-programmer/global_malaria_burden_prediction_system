import json
import logging
import os

import mlflow
import mlflow.artifacts
import mlflow.lightgbm

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

PROPHET_EXPERIMENT_IDS = {
    "AFRO":  "2",
    "AMRO":  "6",
    "EMRO":  "4",
    "SEARO": "3",
    "WPRO":  "5",
}

LIGHTGBM_EXPERIMENT_ID = "1"


def _load_prophet_model(model_uri):
    """
    Load a Prophet model from the MLflow artifact payload.

    The Prophet artifacts in this project are stored as a JSON string that
    contains another JSON string. Older Prophet releases also attempt to load
    a Stan backend during deserialization, which can fail in this runtime.
    We normalize the payload to a dict and temporarily use a safe backend
    loader so deserialization can complete.
    """
    from prophet.forecaster import Prophet, StanBackendEnum
    from prophet.serialize import model_from_dict

    local_path = mlflow.artifacts.download_artifacts(model_uri)
    pr_file = os.path.join(local_path, "model.pr")

    with open(pr_file, "r") as f:
        payload = f.read()

    for _ in range(2):
        if not isinstance(payload, str):
            break
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            break

    if not isinstance(payload, dict):
        raise ValueError("Prophet artifact did not deserialize to a model dictionary")

    original_load_stan_backend = Prophet._load_stan_backend

    def _safe_load_stan_backend(self, stan_backend):
        if stan_backend is None:
            for backend in StanBackendEnum:
                try:
                    return _safe_load_stan_backend(self, backend.name)
                except Exception:
                    continue
            self.stan_backend = None
            return None

        try:
            self.stan_backend = StanBackendEnum.get_backend_class(stan_backend)()
            return self.stan_backend
        except Exception:
            self.stan_backend = None
            return None

    Prophet._load_stan_backend = _safe_load_stan_backend
    try:
        return model_from_dict(payload)
    finally:
        Prophet._load_stan_backend = original_load_stan_backend


def load_all_models():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()
    models = {}

    # ── Load LightGBM ──────────────────────────────────────────────
    try:
        runs = client.search_runs(
            experiment_ids=[LIGHTGBM_EXPERIMENT_ID],
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
    for region, exp_id in PROPHET_EXPERIMENT_IDS.items():
        try:
            runs = client.search_runs(
                experiment_ids=[exp_id],
                order_by=["start_time DESC"],
                max_results=1
            )
            if runs:
                run_id = runs[0].info.run_id
                model_uri = f"runs:/{run_id}/model"
                models[f"prophet_{region}"] = _load_prophet_model(model_uri)
                models[f"prophet_{region}_run_id"] = run_id
                logger.info(f"Prophet loaded for {region} — run {run_id}")
            else:
                logger.warning(f"No runs found in experiment ID: {exp_id}")
        except Exception as e:
            logger.error(f"Failed to load Prophet for {region}: {e}")

    return models