

import os
import io
import logging
import warnings
import boto3
import pandas as pd
import numpy as np
import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.metrics import (
    f1_score, precision_score, recall_score,
    roc_auc_score, classification_report,
)
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────

S3_BUCKET  = os.getenv("S3_BUCKET", "malaria-forecast-bree")
GOLD_KEY   = "gold/features.parquet"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

FEATURE_COLS = [
    "deaths_lag1", "deaths_lag2", "deaths_lag3",
    "deaths_rolling3",
    "incidence_lag1", "incidence_lag2",
    "year", "is_covid_period",
    "who_region_AFRO", "who_region_AMRO", "who_region_EMRO",
    "who_region_EURO", "who_region_SEARO", "who_region_WPRO",
    "scale_factor",
]

TARGET_COL       = "improving"
SCALE_POS_WEIGHT = 3.15      # same as training — keeps class weighting consistent

# Walk-forward windows: each tuple is (train_end, test_start, test_end)
# Training always starts at 2000; we expand the window each time.
CV_WINDOWS = [
    (2014, 2015, 2016),
    (2016, 2017, 2018),
    (2018, 2019, 2020),
    (2020, 2021, 2022),
]

WHO_REGIONS = ["AFRO", "AMRO", "EMRO", "SEARO", "WPRO"]

# LightGBM hyperparameters — identical to Stage 4 training
# We retrain from scratch on each window; we don't load the registered model.
# Reason: we want to measure how the model would have performed if it had only
# seen data up to that window's cutoff. Loading the Production model (trained
# on 2000-2018) and testing on 2015-2016 would be data leakage.
LGBM_PARAMS = {
    "objective":        "binary",
    "metric":           "binary_logloss",
    "n_estimators":     500,
    "learning_rate":    0.05,
    "num_leaves":       31,
    "min_child_samples": 20,
    "scale_pos_weight": SCALE_POS_WEIGHT,
    "random_state":     42,
    "n_jobs":           -1,
    "verbose":          -1,
}


# ── Data loading ───────────────────────────────────────────────────────────────

def load_and_prepare() -> pd.DataFrame:
    """Load gold Parquet from S3, one-hot encode region, drop nulls."""
    log.info("Loading gold data from S3...")
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    # One-hot encode WHO region
    df = pd.get_dummies(df, columns=["who_region"], prefix="who_region")

    # Add any missing region columns
    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = 0

    # Drop rows where any feature or target is null
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    log.info(f"  Ready: {len(df):,} rows, years {df['year'].min()}–{df['year'].max()}")
    return df


# ── Walk-forward cross validation ─────────────────────────────────────────────

def run_cv_window(
    df: pd.DataFrame,
    train_end: int,
    test_start: int,
    test_end: int,
) -> dict:
   
    
    train_df = df[(df["year"] >= 2000) & (df["year"] <= train_end)]
    test_df  = df[(df["year"] >= test_start) & (df["year"] <= test_end)]

    X_train, y_train = train_df[FEATURE_COLS], train_df[TARGET_COL]
    X_test,  y_test  = test_df[FEATURE_COLS],  test_df[TARGET_COL]

    log.info(
        f"  Window {train_end}/{test_start}-{test_end}: "
        f"train={len(X_train):,}  test={len(X_test):,}"
    )

    model = lgb.LGBMClassifier(**LGBM_PARAMS)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    y_pred      = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]

    return {
        "window":    f"{test_start}-{test_end}",
        "train_end": train_end,
        "train_rows": len(X_train),
        "test_rows":  len(X_test),
        "f1":        round(f1_score(y_test, y_pred,      zero_division=0), 4),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall":    round(recall_score(y_test, y_pred,   zero_division=0), 4),
        "roc_auc":   round(roc_auc_score(y_test, y_pred_prob),              4),
    }


def run_walk_forward_cv(df: pd.DataFrame) -> pd.DataFrame:
    """Run all 4 CV windows. Return results as a DataFrame."""
    log.info("\nWalk-forward cross validation:")
    results = []
    for train_end, test_start, test_end in CV_WINDOWS:
        result = run_cv_window(df, train_end, test_start, test_end)
        results.append(result)
        log.info(
            f"    F1={result['f1']:.3f}  AUC={result['roc_auc']:.3f}  "
            f"P={result['precision']:.3f}  R={result['recall']:.3f}"
        )

    cv_df = pd.DataFrame(results)

    log.info("\nSummary across windows:")
    for metric in ["f1", "precision", "recall", "roc_auc"]:
        mean = cv_df[metric].mean()
        std  = cv_df[metric].std()
        log.info(f"  {metric:<12}  mean={mean:.3f}  std={std:.3f}")

    return cv_df


def plot_cv_results(cv_df: pd.DataFrame) -> str:
  
    fig, ax = plt.subplots(figsize=(8, 4))

    ax.plot(cv_df["window"], cv_df["f1"],      marker="o", label="F1",      linewidth=2)
    ax.plot(cv_df["window"], cv_df["roc_auc"], marker="s", label="ROC-AUC", linewidth=2, linestyle="--")
    ax.plot(cv_df["window"], cv_df["precision"], marker="^", label="Precision", linewidth=1.5, alpha=0.7)
    ax.plot(cv_df["window"], cv_df["recall"],    marker="v", label="Recall",    linewidth=1.5, alpha=0.7)

    # Shade the ±1 std band around F1
    mean_f1 = cv_df["f1"].mean()
    std_f1  = cv_df["f1"].std()
    ax.axhline(mean_f1, color="steelblue", linewidth=0.8, linestyle=":", alpha=0.6)
    ax.fill_between(
        cv_df["window"],
        mean_f1 - std_f1,
        mean_f1 + std_f1,
        alpha=0.12, color="steelblue",
        label=f"F1 ±1 std  ({mean_f1:.3f} ± {std_f1:.3f})"
    )

    ax.set_ylim(0, 1.05)
    ax.set_xlabel("Test window")
    ax.set_ylabel("Score")
    ax.set_title("Walk-forward cross validation — LightGBM classifier", fontsize=11)
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    path = "/tmp/cv_results.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


# ── Bias / fairness audit ─────────────────────────────────────────────────────

def run_bias_audit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Load the Production model from MLflow.
    Evaluate it on the test set (2019+) broken down by WHO region.

    We use the Production model here — not a retrained one — because we want
    to know how the deployed model performs per region, not a hypothetical one.
    """
    log.info("\nBias audit — loading Production model...")
    mlflow.set_tracking_uri(MLFLOW_URI)
    model = mlflow.lightgbm.load_model(f"models:/malaria_lgbm_classifier/Production")

    test_df = df[df["year"] >= 2019].copy()
    X_test  = test_df[FEATURE_COLS]
    y_test  = test_df[TARGET_COL]

    # Recover region label from one-hot columns
    region_cols    = [c for c in test_df.columns if c.startswith("who_region_")]
    test_df["region"] = (
        test_df[region_cols]
        .idxmax(axis=1)
        .str.replace("who_region_", "")
    )

    y_pred      = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]

    test_df = test_df.copy()
    test_df["y_pred"]      = y_pred
    test_df["y_pred_prob"] = y_pred_prob

    rows = []
    for region in WHO_REGIONS:
        mask = test_df["region"] == region
        if mask.sum() < 5:
            continue

        yt = test_df.loc[mask, TARGET_COL]
        yp = test_df.loc[mask, "y_pred"]
        ypp = test_df.loc[mask, "y_pred_prob"]

        # Guard against regions with only one class in test set
        try:
            auc = round(roc_auc_score(yt, ypp), 4)
        except ValueError:
            auc = float("nan")

        rows.append({
            "region":    region,
            "n_rows":    int(mask.sum()),
            "pct_improving": round(yt.mean() * 100, 1),
            "f1":        round(f1_score(yt, yp,        zero_division=0), 4),
            "precision": round(precision_score(yt, yp, zero_division=0), 4),
            "recall":    round(recall_score(yt, yp,    zero_division=0), 4),
            "roc_auc":   auc,
        })

        log.info(
            f"  {region:<8}  n={mask.sum():>4}  "
            f"F1={rows[-1]['f1']:.3f}  AUC={auc:.3f}  "
            f"improving={rows[-1]['pct_improving']}%"
        )

    bias_df = pd.DataFrame(rows)

    # Flag regions where F1 < 0.40 — model has effectively given up there
    bias_df["flagged"] = bias_df["f1"] < 0.40
    flagged = bias_df[bias_df["flagged"]]["region"].tolist()
    if flagged:
        log.warning(f"\n  *** BIAS FLAG: F1 < 0.40 in regions: {flagged} ***")
        log.warning("      Model may not be reliable for these regions.")
    else:
        log.info("\n  No regions flagged (all F1 >= 0.40)")

    return bias_df


def plot_bias_audit(bias_df: pd.DataFrame) -> str:
    """
    Grouped bar chart: F1, Precision, Recall per WHO region.
    Flagged regions (F1 < 0.40) are highlighted in red.
    """
    regions = bias_df["region"].tolist()
    x       = np.arange(len(regions))
    width   = 0.25

    fig, ax = plt.subplots(figsize=(9, 5))
    b1 = ax.bar(x - width, bias_df["f1"],        width, label="F1",        alpha=0.85)
    b2 = ax.bar(x,          bias_df["precision"], width, label="Precision", alpha=0.85)
    b3 = ax.bar(x + width,  bias_df["recall"],    width, label="Recall",    alpha=0.85)

    # Highlight flagged region bars in red
    for i, (_, row) in enumerate(bias_df.iterrows()):
        if row["flagged"]:
            for bar_group in [b1, b2, b3]:
                bar_group[i].set_color("crimson")
                bar_group[i].set_alpha(0.9)

    ax.set_xticks(x)
    ax.set_xticklabels(regions)
    ax.set_ylim(0, 1.1)
    ax.set_ylabel("Score")
    ax.set_title("Bias audit — model performance by WHO region\n"
                 "Red bars = region flagged (F1 < 0.40)", fontsize=11)
    ax.legend()
    ax.axhline(0.40, color="crimson", linewidth=0.8, linestyle="--", alpha=0.5, label="Flag threshold")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    path = "/tmp/bias_audit.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path



def run_validation():
    log.info("=" * 60)
    log.info("Stage 5 — LightGBM Validation")
    log.info("=" * 60)

    df = load_and_prepare()

    # ── Cross validation ──
    cv_df      = run_walk_forward_cv(df)
    cv_plot    = plot_cv_results(cv_df)

    # ── Bias audit ──
    bias_df    = run_bias_audit(df)
    bias_plot  = plot_bias_audit(bias_df)

    # ── Save summary CSVs to /tmp ──
    cv_csv_path   = "/tmp/cv_results.csv"
    bias_csv_path = "/tmp/bias_audit.csv"
    cv_df.to_csv(cv_csv_path,   index=False)
    bias_df.to_csv(bias_csv_path, index=False)

    # ── Log everything to MLflow ──
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("malaria_stage5_validation")

    with mlflow.start_run(run_name="lightgbm_validation"):

        # CV summary metrics
        for metric in ["f1", "precision", "recall", "roc_auc"]:
            mlflow.log_metric(f"cv_mean_{metric}", round(cv_df[metric].mean(), 4))
            mlflow.log_metric(f"cv_std_{metric}",  round(cv_df[metric].std(),  4))

        # Per-window metrics
        for _, row in cv_df.iterrows():
            w = row["window"].replace("-", "_")
            mlflow.log_metric(f"cv_f1_{w}",      row["f1"])
            mlflow.log_metric(f"cv_roc_auc_{w}", row["roc_auc"])

        # Bias audit metrics
        for _, row in bias_df.iterrows():
            r = row["region"]
            mlflow.log_metric(f"bias_f1_{r}",      row["f1"])
            mlflow.log_metric(f"bias_recall_{r}",  row["recall"])
            mlflow.log_metric(f"bias_auc_{r}",     row["roc_auc"])

        flagged = bias_df[bias_df["flagged"]]["region"].tolist()
        mlflow.log_param("bias_flagged_regions", str(flagged) if flagged else "none")

        # Artifacts
        mlflow.log_artifact(cv_plot,       artifact_path="validation_plots")
        mlflow.log_artifact(bias_plot,     artifact_path="validation_plots")
        mlflow.log_artifact(cv_csv_path,   artifact_path="validation_data")
        mlflow.log_artifact(bias_csv_path, artifact_path="validation_data")

        log.info(f"\nAll results logged to MLflow experiment: malaria_stage5_validation")

    log.info("\nValidation complete.")


if __name__ == "__main__":
    run_validation()