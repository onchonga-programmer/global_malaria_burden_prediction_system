from pydantic import BaseModel
from typing import List, Dict, Optional

class ForecastPoint(BaseModel):
    year: int
    predicted_deaths: float
    lower_bound: float
    upper_bound: float

class AnomalyPoint(BaseModel):
    year: int
    actual_deaths: float
    is_anomaly: bool
    anomaly_score: float

class ForecastResponse(BaseModel):
    region: str
    forecast: List[ForecastPoint]
    shap_values: Dict[str, float]
    anomalies: List[AnomalyPoint]
    model_version: str

class ClassifyResponse(BaseModel):
    region: str
    prediction: str          # "improving" or "deteriorating"
    probability: float       # confidence 0.0 to 1.0
    shap_values: Dict[str, float]
    model_version: str

class HealthResponse(BaseModel):
    status: str
    models_loaded: List[str]