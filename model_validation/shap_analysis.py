

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
import matplotlib
matplotlib.use("Agg")          
import matplotlib.pyplot as plt
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

# ── Configuration 

S3_BUCKET        = os.getenv("S3_BUCKET", "malaria-forecast-bree")
GOLD_KEY         = "gold/malaria_features/features.parquet"
MLFLOW_URI       = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME       = "malaria_lgbm_classifier"

FEATURE_COLS = [
    "deaths_lag1", "deaths_lag2", "deaths_lag3",
    "deaths_rolling3",
    "incidence_lag1", "incidence_lag2",
    "year", "is_covid_period",
    "who_region_AFRO", "who_region_AMRO", "who_region_EMRO",
    "who_region_EURO", "who_region_SEARO", "who_region_WPRO",
    "scale_factor",
]

TARGET_COL   = "improving"
TEST_CUTOFF  = 2019          
WHO_REGIONS  = ["AFRO", "AMRO", "EMRO", "SEARO", "WPRO"]



def load_gold_data() -> pd.DataFrame:
    log.info("Loading gold data from S3...")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    log.info(f"  Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


def prepare_test_set(df: pd.DataFrame):
   
    df = df.copy()

    # One-hot encode WHO region 
    df = pd.get_dummies(df, columns=["who_region"], prefix="who_region")

    # Add any missing region columns (e.g. EURO missing if filtered)
    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = 0

    # Drop rows where any feature is null
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    # Split
    test_df = df[df["year"] >= TEST_CUTOFF].copy()
    log.info(f"  Test set: {len(test_df):,} rows from {test_df['year'].min()}–{test_df['year'].max()}")
    return test_df


def load_production_model():
    log.info(f"Loading model '{MODEL_NAME}' from MLflow registry...")
    mlflow.set_tracking_uri(MLFLOW_URI)
    model_uri = f"models:/{MODEL_NAME}/Production"
    model = mlflow.lightgbm.load_model(model_uri)
    log.info("  Model loaded successfully")
    return model


def save_figure(fig, filename: str) -> str:
    """Save a matplotlib figure to /tmp and return the path."""
    path = f"/tmp/{filename}"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


# ── Level 1: Global beeswarm ───────────────────────────────────────────────────

def plot_global_beeswarm(shap_values, X_test: pd.DataFrame) -> str:
    
    log.info("Plotting Level 1: global beeswarm...")

    fig, ax = plt.subplots(figsize=(10, 7))
    shap.plots.beeswarm(
        shap_values,
        max_display=15,          # show all 15 features
        show=False,
        ax=ax,
    )
    ax.set_title(
        "Global SHAP feature importance\n"
        "Each dot = one country-year  |  Position = impact on prediction  |  Colour = feature value",
        fontsize=11, pad=12
    )
    fig.tight_layout()
    path = save_figure(fig, "shap_global_beeswarm.png")
    log.info(f"  Saved → {path}")
    return path


# ── Level 2: Per-region bar charts ────────────────────────────────────────────

def plot_per_region_bars(shap_values, X_test: pd.DataFrame, test_df: pd.DataFrame) -> list[str]:
   
    log.info("Plotting Level 2: per-region SHAP bars...")

    # shap_values.values is shape (n_rows, n_features)
    shap_array = shap_values.values
    paths = []

    # We need the who_region column from test_df (before one-hot encoding)
    # It was stored as who_region_AFRO etc., so we recover it
    region_series = test_df[
        [c for c in test_df.columns if c.startswith("who_region_")]
    ].idxmax(axis=1).str.replace("who_region_", "")

    for region in WHO_REGIONS:
        mask = (region_series == region).values
        if mask.sum() < 5:
            log.warning(f"  Skipping {region} — only {mask.sum()} rows")
            continue

        region_shap = shap_array[mask]                   # subset to this region
        mean_abs    = np.abs(region_shap).mean(axis=0)   # mean |SHAP| per feature
        importance  = pd.Series(mean_abs, index=X_test.columns)
        importance  = importance.sort_values(ascending=True).tail(10)  # top 10

        fig, ax = plt.subplots(figsize=(8, 5))
        bars = ax.barh(importance.index, importance.values, color="#4E79A7", alpha=0.85)
        ax.set_xlabel("Mean |SHAP value|  (higher = more important for this region)", fontsize=10)
        ax.set_title(f"SHAP feature importance — {region}\n"
                     f"({mask.sum()} country-year rows in test set)", fontsize=11)
        ax.bar_label(bars, fmt="%.3f", padding=3, fontsize=9)
        fig.tight_layout()
        filename = f"shap_region_{region}.png"
        path = save_figure(fig, filename)
        paths.append(path)
        log.info(f"  {region}: saved → {path}")

    return paths


# ── Level 3: Per-country waterfall ────────────────────────────────────────────

def plot_country_waterfall(
    shap_values,
    X_test: pd.DataFrame,
    test_df: pd.DataFrame,
    country: str,
    year: int,
) -> str | None:
  
    log.info(f"Plotting Level 3: waterfall for {country} {year}...")

    # Find this specific row in the test set
    row_mask = (test_df["country"] == country) & (test_df["year"] == year)

    if row_mask.sum() == 0:
        log.warning(f"  {country} {year} not found in test set — skipping waterfall")
        return None

    row_idx = row_mask.idxmax()                              # index in test_df
    # We need the positional index in shap_values
    pos_idx = test_df.index.get_loc(row_idx)

    actual    = int(test_df.loc[row_idx, TARGET_COL])
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
        fontsize=11, pad=12
    )
    fig.tight_layout()
    filename = f"shap_waterfall_{country.replace(' ', '_')}_{year}.png"
    path = save_figure(fig, filename)
    log.info(f"  Saved → {path}")
    return path


# ── Main ───────────────────────────────────────────────────────────────────────

def run_shap_analysis():
 
    log.info("=" * 60)
    log.info("Stage 5 — SHAP Deep Analysis")
    log.info("=" * 60)

    # ── Load ──
    df       = load_gold_data()
    test_df  = prepare_test_set(df)
    model    = load_production_model()

    X_test = test_df[FEATURE_COLS]
    y_test = test_df[TARGET_COL]

  
    log.info("Computing SHAP values with TreeExplainer...")
    log.info("  (This is the slowest step — ~30s for 10k rows)")
    explainer   = shap.TreeExplainer(model)
    shap_values = explainer(X_test)
    log.info(f"  SHAP values computed: {shap_values.values.shape}")

    # ── Generate plots ───────────────────────────────────────────────────────
    beeswarm_path = plot_global_beeswarm(shap_values, X_test)

    region_paths  = plot_per_region_bars(shap_values, X_test, test_df)

    
    waterfall_countries = [
        ("Nigeria",  2022),
        ("Thailand", 2022),
        ("Yemen",    2022),
    ]
    waterfall_paths = []
    for country, year in waterfall_countries:
        path = plot_country_waterfall(shap_values, X_test, test_df, country, year)
        if path:
            waterfall_paths.append(path)

    # ── Log to MLflow ────────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("malaria_stage5_validation")

    with mlflow.start_run(run_name="shap_deep_analysis"):

        # Log a summary metric: mean absolute SHAP of top feature
        top_feature_importance = float(
            np.abs(shap_values.values).mean(axis=0).max()
        )
        mlflow.log_metric("shap_top_feature_mean_abs", top_feature_importance)

        # Log which feature is most important globally
        feature_importance = pd.Series(
            np.abs(shap_values.values).mean(axis=0),
            index=FEATURE_COLS
        ).sort_values(ascending=False)
        top_feature = feature_importance.index[0]
        mlflow.log_param("shap_top_global_feature", top_feature)

        log.info(f"\nTop 5 features by global mean |SHAP|:")
        for feat, val in feature_importance.head(5).items():
            log.info(f"  {feat:<30} {val:.4f}")
            mlflow.log_metric(f"shap_mean_abs_{feat}", round(val, 4))

        # Log all plot artifacts
        mlflow.log_artifact(beeswarm_path,  artifact_path="shap_plots/global")
        for p in region_paths:
            mlflow.log_artifact(p, artifact_path="shap_plots/regions")
        for p in waterfall_paths:
            mlflow.log_artifact(p, artifact_path="shap_plots/waterfalls")

        log.info("\nAll SHAP plots logged to MLflow")
        log.info(f"  Experiment: malaria_stage5_validation")
        log.info(f"  View at:    {MLFLOW_URI}")

    log.info("\nSHAP analysis complete.")


if __name__ == "__main__":
    run_shap_analysis()