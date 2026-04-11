from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import pandas as pd
import numpy as np
import shap
import logging
import os

from schema import ForecastResponse, ClassifyResponse, HealthResponse, ForecastPoint, AnomalyPoint
from model_loader import load_all_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This dictionary holds all loaded models
# It lives outside any function so all endpoints can access it
ml_models = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Everything BEFORE yield runs at startup
    logger.info("Loading models from MLflow...")
    loaded = load_all_models()
    ml_models.update(loaded)
    logger.info(f"Models loaded: {[k for k in ml_models if not k.endswith('_run_id')]}")
    
    yield  # Server is now running and accepting requests
    
    # Everything AFTER yield runs at shutdown
    ml_models.clear()
    logger.info("Models cleared from memory")

app = FastAPI(
    title="Malaria Burden Prediction API",
    description="Forecasts malaria deaths by WHO region and classifies trend direction",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health", response_model=HealthResponse)
def health_check():
    loaded_names = [k for k in ml_models if not k.endswith("_run_id")]
    return HealthResponse(
        status="ok",
        models_loaded=loaded_names
    )

@app.get("/forecast", response_model=ForecastResponse)
def get_forecast(region: str):
    region = region.upper()
    model_key = f"prophet_{region}"
    
    if model_key not in ml_models:
        raise HTTPException(
            status_code=404,
            detail=f"No Prophet model loaded for region '{region}'. "
                   f"Available regions: AFRO, AMRO, EMRO, EURO, SEARO, WPRO"
        )
    
    prophet_model = ml_models[model_key]
    
    # Create future dataframe for next 5 years
    future = pd.DataFrame({
        "ds": pd.date_range(start="2025-01-01", periods=5, freq="A")
    })
    
    forecast_df = prophet_model.predict(future)
    
    forecast_points = [
        ForecastPoint(
            year=row["ds"].year,
            predicted_deaths=max(0, row["yhat"]),       # deaths can't be negative
            lower_bound=max(0, row["yhat_lower"]),
            upper_bound=max(0, row["yhat_upper"])
        )
        for _, row in forecast_df.iterrows()
    ]
    
    # SHAP for Prophet: use component contributions as explainability
    shap_values = {
        "trend": float(forecast_df["trend"].mean()),
        "yearly_seasonality": float(forecast_df.get("yearly", pd.Series([0])).mean()),
    }
    
    # Placeholder anomalies — you'll wire in your Stage 5 anomaly detector
    anomalies = []
    
    run_id = ml_models.get(f"prophet_{region}_run_id", "unknown")
    
    return ForecastResponse(
        region=region,
        forecast=forecast_points,
        shap_values=shap_values,
        anomalies=anomalies,
        model_version=run_id
    )

@app.get("/classify", response_model=ClassifyResponse)
def get_classification(region: str):
    region = region.upper()
    
    if "lightgbm" not in ml_models:
        raise HTTPException(status_code=503, detail="LightGBM classifier not loaded")
    
    lgbm_model = ml_models["lightgbm"]
    
    # Build a feature row for the most recent year of this region
    # In production you'd read this from S3 gold layer
    # For now we use a representative feature set
    feature_row = _get_latest_features(region)
    
    if feature_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No feature data found for region '{region}'"
        )
    
    X = pd.DataFrame([feature_row])
    
    # Prediction
    probability = float(lgbm_model.predict_proba(X)[0][1])
    prediction = "improving" if probability >= 0.5 else "deteriorating"
    
    # SHAP explanation
    explainer = shap.TreeExplainer(lgbm_model)
    shap_vals = explainer.shap_values(X)
    
    # shap_vals shape depends on whether it's binary: take class 1
    if isinstance(shap_vals, list):
        shap_arr = shap_vals[1][0]
    else:
        shap_arr = shap_vals[0]
    
    shap_dict = {
        col: round(float(val), 4)
        for col, val in zip(X.columns, shap_arr)
    }
    
    run_id = ml_models.get("lightgbm_run_id", "unknown")
    
    return ClassifyResponse(
        region=region,
        prediction=prediction,
        probability=round(probability, 4),
        shap_values=shap_dict,
        model_version=run_id
    )

def _get_latest_features(region: str) -> dict | None:
    """
    Returns a representative feature row for the given region.
    Features match exactly what the LightGBM model was trained on.
    
    In production this would read the latest row from S3 gold layer.
    Values here are regional averages used as a reasonable placeholder.
    """
    # Region encoding — matches who_region_encoded from training
    region_encoding = {
        "AFRO": 0, "AMRO": 1, "EMRO": 2,
        "EURO": 3, "SEARO": 4, "WPRO": 5
    }

    if region not in region_encoding:
        return None

    # Representative values based on AFRO-scale malaria burden
    # These are placeholders — real serving would pull from S3
    return {
        "deaths_lag1":                400000.0,
        "deaths_lag2":                420000.0,
        "deaths_lag3":                440000.0,
        "deaths_rolling3":            420000.0,
        "death_rate_per_100k_lag1":   45.0,
        "death_rate_per_100k_lag2":   47.0,
        "death_rate_per_100k_lag3":   49.0,
        "death_rate_per_100k_rolling3": 47.0,
        "incidence_per_1000":         220.0,
        "incidence_per_1000_lag1":    230.0,
        "incidence_per_1000_lag2":    240.0,
        "incidence_per_1000_lag3":    250.0,
        "who_region_encoded":         float(region_encoding[region]),
        "is_covid_period":            0.0,
        "year_normalized":            0.8,   # ~2022 normalized
    }