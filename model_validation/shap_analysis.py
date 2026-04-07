import os
import io
import logging
import warnings
import boto3
import pandas as pd
import numpy as np
import shap
import mlflow
import mlflow.lightgbm
from mlflow.exceptions import MlflowException
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "malaria-forecast-bree")
GOLD_KEY = "gold/malaria_features/features.parquet"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = "malaria_lgbm_classifier"

FEATURE_COLS = [
    "deaths_lag1",
    "deaths_lag2",
    "deaths_lag3",
    "deaths_rolling3",
    "death_rate_per_100k_lag1",
    "death_rate_per_100k_lag2",
    "death_rate_per_100k_lag3",
    "death_rate_per_100k_rolling3",
    "incidence_per_1000",
    "incidence_per_1000_lag1",
    "incidence_per_1000_lag2",
    "incidence_per_1000_lag3",
    "who_region_encoded",
    "is_covid_period",
    "year_normalized",
]

TARGET_COL = "improving"
TEST_CUTOFF = 2019
WHO_REGIONS = ["AFRO", "AMRO", "EMRO", "SEARO", "WPRO"]
REGION_MAP = {
    "AFRO": 0,
    "SEARO": 1,
    "EMRO": 2,
    "WPRO": 3,
    "AMRO": 4,
    "EURO": 5,
    "OTHER": 6,
    "Other": 6,
}


def load_gold_data() -> pd.DataFrame:
    log.info("Loading gold data from S3...")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    log.info(f"  Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


def prepare_test_set(df: pd.DataFrame):
    df = df.copy()

    # Keep feature engineering aligned with stage 4 training.
    if "is_covid_period" not in df.columns:
        df["is_covid_period"] = (df["year"] >= 2020).astype(int)
    if "who_region_encoded" not in df.columns:
        df["who_region_encoded"] = df["who_region"].map(REGION_MAP).fillna(6).astype(int)

    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = 0

    # Match stage 4 training preparation (country-level rows only).
    if "is_aggregate_region" in df.columns:
        df = df[df["is_aggregate_region"] == False].copy()

    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    test_df = df[df["year"] >= TEST_CUTOFF].copy()
    log.info(f"  Test set: {len(test_df):,} rows from {test_df['year'].min()}–{test_df['year'].max()}")
    return test_df


def load_production_model():
    log.info(f"Loading model '{MODEL_NAME}' from MLflow registry...")
    mlflow.set_tracking_uri(MLFLOW_URI)
    model_uri = f"models:/{MODEL_NAME}/Production"

    try:
        model = mlflow.lightgbm.load_model(model_uri)
        log.info("  Loaded Production model")
        return model
    except MlflowException as exc:
        log.warning(f"  Production stage not available: {exc}")

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        fallback_uri = f"models:/{MODEL_NAME}/{latest.version}"
        model = mlflow.lightgbm.load_model(fallback_uri)
        log.info(f"  Loaded fallback registry version: {latest.version}")
        return model

    exp = mlflow.get_experiment_by_name("malaria_lightgbm")
    if exp is not None:
        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["start_time DESC"],
            max_results=30,
        )
        for _, row in runs.iterrows():
            run_id = row["run_id"]
            run_uri = f"runs:/{run_id}/model"
            try:
                model = mlflow.lightgbm.load_model(run_uri)
                log.info(f"  Loaded fallback run artifact model from run {run_id[:8]}...")
                return model
            except Exception:
                continue

    raise RuntimeError(
        "No LightGBM model found in MLflow registry or runs. "
        "Run stage 4 training first to log a model artifact."
    )


def save_figure(fig, filename: str) -> str:
    path = f"/tmp/{filename}"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def plot_global_beeswarm(shap_values, X_test: pd.DataFrame) -> str:
    log.info("Plotting Level 1: global beeswarm...")

    plt.figure(figsize=(10, 7))
    shap.plots.beeswarm(
        shap_values,
        max_display=15,
        show=False,
    )
    plt.title(
        "Global SHAP feature importance\n"
        "Each dot = one country-year  |  Position = impact on prediction  |  Colour = feature value",
        fontsize=11,
        pad=12,
    )
    fig = plt.gcf()
    fig.tight_layout()
    path = save_figure(fig, "shap_global_beeswarm.png")
    log.info(f"  Saved → {path}")
    return path


def plot_per_region_bars(shap_values, X_test: pd.DataFrame, test_df: pd.DataFrame) -> list[str]:
    log.info("Plotting Level 2: per-region SHAP bars...")

    shap_array = shap_values.values
    paths = []
    region_series = test_df["who_region"].astype(str)

    for region in WHO_REGIONS:
        mask = (region_series == region).values
        if mask.sum() < 5:
            log.warning(f"  Skipping {region} — only {mask.sum()} rows")
            continue

        region_shap = shap_array[mask]
        mean_abs = np.abs(region_shap).mean(axis=0)
        importance = pd.Series(mean_abs, index=X_test.columns)
        importance = importance.sort_values(ascending=True).tail(10)

        fig, ax = plt.subplots(figsize=(8, 5))
        bars = ax.barh(importance.index, importance.values, color="#4E79A7", alpha=0.85)
        ax.set_xlabel("Mean |SHAP value|  (higher = more important for this region)", fontsize=10)
        ax.set_title(
            f"SHAP feature importance — {region}\n"
            f"({mask.sum()} country-year rows in test set)",
            fontsize=11,
        )
        ax.bar_label(bars, fmt="%.3f", padding=3, fontsize=9)
        fig.tight_layout()
        filename = f"shap_region_{region}.png"
        path = save_figure(fig, filename)
        paths.append(path)
        log.info(f"  {region}: saved → {path}")

    return paths


def plot_country_waterfall(
    shap_values,
    X_test: pd.DataFrame,
    test_df: pd.DataFrame,
    country: str,
    year: int,
) -> str | None:
    log.info(f"Plotting Level 3: waterfall for {country} {year}...")

    row_mask = (test_df["country"] == country) & (test_df["year"] == year)

    if row_mask.sum() == 0:
        log.warning(f"  {country} {year} not found in test set — skipping waterfall")
        return None

    row_idx = row_mask.idxmax()
    pos_idx = test_df.index.get_loc(row_idx)

    actual = int(test_df.loc[row_idx, TARGET_COL])
    label_str = "IMPROVING" if actual == 1 else "NOT IMPROVING"

    fig, ax = plt.subplots(figsize=(9, 6))
    shap.plots.waterfall(
        shap_values[pos_idx],
        max_display=15,
        show=False,
    )
    plt.title(
        f"SHAP waterfall — {country}, {year}\n"
        f"Actual label: {label_str}",
        fontsize=11,
        pad=12,
    )
    fig.tight_layout()
    filename = f"shap_waterfall_{country.replace(' ', '_')}_{year}.png"
    path = save_figure(fig, filename)
    log.info(f"  Saved → {path}")
    return path


def run_shap_analysis():
    log.info("=" * 60)
    log.info("Stage 5 — SHAP Deep Analysis")
    log.info("=" * 60)

    df = load_gold_data()
    test_df = prepare_test_set(df)
    model = load_production_model()

    X_test = test_df[FEATURE_COLS]

    log.info("Computing SHAP values with TreeExplainer...")
    log.info("  (This is the slowest step — ~30s for 10k rows)")
    explainer = shap.TreeExplainer(model)
    shap_values = explainer(X_test)
    if hasattr(shap_values, "values") and shap_values.values.ndim == 3:
        # Binary classifiers may return per-class SHAP values; use positive class.
        shap_values = shap_values[:, :, 1]
    log.info(f"  SHAP values computed: {shap_values.values.shape}")

    beeswarm_path = plot_global_beeswarm(shap_values, X_test)
    region_paths = plot_per_region_bars(shap_values, X_test, test_df)

    waterfall_countries = [
        ("Nigeria", 2022),
        ("Thailand", 2022),
        ("Yemen", 2022),
    ]
    waterfall_paths = []
    for country, year in waterfall_countries:
        path = plot_country_waterfall(shap_values, X_test, test_df, country, year)
        if path:
            waterfall_paths.append(path)

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("malaria_stage5_validation")

    with mlflow.start_run(run_name="shap_deep_analysis"):
        top_feature_importance = float(np.abs(shap_values.values).mean(axis=0).max())
        mlflow.log_metric("shap_top_feature_mean_abs", top_feature_importance)

        feature_importance = pd.Series(
            np.abs(shap_values.values).mean(axis=0),
            index=FEATURE_COLS,
        ).sort_values(ascending=False)
        top_feature = feature_importance.index[0]
        mlflow.log_param("shap_top_global_feature", top_feature)

        log.info("\nTop 5 features by global mean |SHAP|:")
        for feat, val in feature_importance.head(5).items():
            log.info(f"  {feat:<30} {val:.4f}")
            mlflow.log_metric(f"shap_mean_abs_{feat}", round(val, 4))

        mlflow.log_artifact(beeswarm_path, artifact_path="shap_plots/global")
        for p in region_paths:
            mlflow.log_artifact(p, artifact_path="shap_plots/regions")
        for p in waterfall_paths:
            mlflow.log_artifact(p, artifact_path="shap_plots/waterfalls")

        log.info("\nAll SHAP plots logged to MLflow")
        log.info("  Experiment: malaria_stage5_validation")
        log.info(f"  View at:    {MLFLOW_URI}")

    log.info("\nSHAP analysis complete.")


if __name__ == "__main__":
    run_shap_analysis()
