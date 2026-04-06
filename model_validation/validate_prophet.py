

import os
import io
import logging
import warnings
import boto3
import pandas as pd
import numpy as np
import mlflow
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

load_dotenv()


S3_BUCKET  = os.getenv("S3_BUCKET", "malaria-forecast-bree")
GOLD_KEY   = "gold/malaria_features/features.parquet"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

WHO_REGIONS = ["AFRO", "AMRO", "EMRO", "SEARO", "WPRO"]

CV_INITIAL = "3650 days"    
CV_PERIOD  = "730 days"     
CV_HORIZON = "1095 days"    


PROPHET_CI_LEVEL = 0.80     


# ── Data loading ───────────────────────────────────────────────────────────────

def load_regional_series() -> dict[str, pd.DataFrame]:
  
    log.info("Loading gold data from S3...")
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))

   
    region_df = df[df["is_aggregate_region"] == True].copy()

    regional_series = {}
    for region in WHO_REGIONS:
        r_df = (
            region_df[region_df["who_region"] == region][["year", "deaths"]]
            .dropna()
            .sort_values("year")
            .copy()
        )
        r_df["ds"] = pd.to_datetime(r_df["year"].astype(str) + "-01-01")
        r_df["y"]  = r_df["deaths"].astype(float)
        r_df       = r_df[["ds", "y"]].dropna()

        if len(r_df) < 10:
            log.warning(f"  {region}: only {len(r_df)} rows — skipping")
            continue

        regional_series[region] = r_df
        log.info(f"  {region}: {len(r_df)} annual rows, {r_df['y'].min():,.0f}–{r_df['y'].max():,.0f} deaths")

    return regional_series


# ── Prophet cross validation

def validate_region(region: str, series_df: pd.DataFrame) -> dict:
    

    log.info(f"\n  Validating {region} ({len(series_df)} rows)...")

   
    model = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=PROPHET_CI_LEVEL,
        seasonality_mode="additive",
    )
    model.fit(series_df)

   
    try:
        df_cv = cross_validation(
            model,
            initial=CV_INITIAL,
            period=CV_PERIOD,
            horizon=CV_HORIZON,
            parallel=None,        
        )
    except Exception as e:
        log.warning(f"    CV failed for {region}: {e}")
        return {"region": region, "status": "cv_failed"}

  
    df_perf = performance_metrics(df_cv, rolling_window=1.0)

    mape = float(df_perf["mape"].mean())    
    mae  = float(df_perf["mae"].mean())
    rmse = float(df_perf["rmse"].mean())

    inside = (
        (df_cv["y"] >= df_cv["yhat_lower"]) &
        (df_cv["y"] <= df_cv["yhat_upper"])
    )
    coverage = float(inside.mean())

    if coverage >= 0.70:
        cal_verdict = "well_calibrated"
    elif coverage >= 0.55:
        cal_verdict = "slightly_overconfident"
    else:
        cal_verdict = "overconfident"

    log.info(f"    MAPE={mape:.3f}  MAE={mae:,.0f}  Coverage={coverage:.2f}  → {cal_verdict}")

    return {
        "region":       region,
        "status":       "ok",
        "mape":         round(mape,     4),
        "mae":          round(mae,      1),
        "rmse":         round(rmse,     1),
        "ci_coverage":  round(coverage, 3),
        "ci_verdict":   cal_verdict,
        "n_cv_rows":    len(df_cv),
        "n_windows":    df_cv["cutoff"].nunique(),
        "df_cv":        df_cv,    
    }


# ── Plotting 

def plot_accuracy_summary(results: list[dict]) -> str:
   
    ok = [r for r in results if r.get("status") == "ok"]
    if not ok:
        return None

    regions = [r["region"] for r in ok]
    mapes   = [r["mape"] * 100 for r in ok]        
    maes    = [r["mae"] for r in ok]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5))

    # MAPE bars
    bars = ax1.barh(regions, mapes, color="#4E79A7", alpha=0.85)
    ax1.set_xlabel("MAPE (%)")
    ax1.set_title("Point forecast error — MAPE\n(% of actual value)", fontsize=11)
    ax1.bar_label(bars, fmt="%.1f%%", padding=3, fontsize=9)
    ax1.axvline(20, color="orange", linewidth=0.8, linestyle="--", alpha=0.7)
    ax1.text(21, -0.4, "20% threshold", fontsize=8, color="orange", alpha=0.8)

    # MAE bars — raw death count error
    bars2 = ax2.barh(regions, maes, color="#F28E2B", alpha=0.85)
    ax2.set_xlabel("MAE (deaths)")
    ax2.set_title("Point forecast error — MAE\n(absolute deaths, regardless of region size)", fontsize=11)
    ax2.bar_label(bars2, fmt="%.0f", padding=3, fontsize=9)

    fig.suptitle(
        "Prophet forecast accuracy per WHO region\n"
        "MAPE is misleading for small regions — read both panels together",
        fontsize=11, y=1.01
    )
    fig.tight_layout()
    path = "/tmp/prophet_accuracy.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def plot_calibration(results: list[dict]) -> str:
  
    ok = [r for r in results if r.get("status") == "ok"]
    if not ok:
        return None

    regions   = [r["region"] for r in ok]
    coverages = [r["ci_coverage"] for r in ok]

    colors = []
    for c in coverages:
        if c >= 0.70:
            colors.append("#59A14F")    
        elif c >= 0.55:
            colors.append("#F28E2B")    
        else:
            colors.append("#E15759")    

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(regions, coverages, color=colors, alpha=0.85)
    ax.axvline(PROPHET_CI_LEVEL, color="steelblue", linewidth=1.5,
               linestyle="--", label=f"Claimed {int(PROPHET_CI_LEVEL*100)}% CI")
    ax.set_xlim(0, 1.05)
    ax.set_xlabel("Actual coverage (fraction of actuals inside CI)")
    ax.set_title(
        f"Interval calibration — does Prophet's {int(PROPHET_CI_LEVEL*100)}% CI hold?\n"
        "Green = well-calibrated  |  Orange = marginal  |  Red = overconfident",
        fontsize=11
    )
    ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=9)
    ax.legend()
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    path = "/tmp/prophet_calibration.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def plot_region_forecast(region: str, df_cv: pd.DataFrame) -> str:
    
    fig, ax = plt.subplots(figsize=(7, 5))

    # Compute horizon in years for colour mapping
    df_cv = df_cv.copy()
    df_cv["horizon_days"] = (df_cv["ds"] - df_cv["cutoff"]).dt.days
    df_cv["horizon_yr"]   = (df_cv["horizon_days"] / 365).round(1)

    scatter = ax.scatter(
        df_cv["y"], df_cv["yhat"],
        c=df_cv["horizon_yr"], cmap="YlOrRd",
        alpha=0.7, s=40, edgecolors="none"
    )

    # Perfect prediction line
    lim_min = min(df_cv["y"].min(), df_cv["yhat"].min()) * 0.9
    lim_max = max(df_cv["y"].max(), df_cv["yhat"].max()) * 1.1
    ax.plot([lim_min, lim_max], [lim_min, lim_max],
            "k--", linewidth=0.8, alpha=0.5, label="Perfect forecast")

    cbar = fig.colorbar(scatter, ax=ax)
    cbar.set_label("Forecast horizon (years)", fontsize=9)

    ax.set_xlabel("Actual deaths")
    ax.set_ylabel("Predicted deaths (yhat)")
    ax.set_title(f"Actual vs predicted — {region}\nColour = how far ahead the forecast was made", fontsize=11)
    ax.legend(fontsize=9)
    fig.tight_layout()

    path = f"/tmp/prophet_actual_vs_pred_{region}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path



def run_prophet_validation():
    log.info("=" * 60)
    log.info("Stage 5 — Prophet Validation")
    log.info("=" * 60)

    regional_series = load_regional_series()

    results = []
    for region, series_df in regional_series.items():
        result = validate_region(region, series_df)
        results.append(result)

    accuracy_plot     = plot_accuracy_summary(results)
    calibration_plot  = plot_calibration(results)

    region_plots = []
    for r in results:
        if r.get("status") == "ok":
            p = plot_region_forecast(r["region"], r["df_cv"])
            region_plots.append(p)

    summary_rows = []
    for r in results:
        row = {k: v for k, v in r.items() if k != "df_cv"}
        summary_rows.append(row)
    summary_df = pd.DataFrame(summary_rows)

    log.info("\nValidation summary:")
    log.info(summary_df[["region", "mape", "mae", "ci_coverage", "ci_verdict"]].to_string(index=False))

    log.info("\nInterpretation notes:")
    for r in results:
        if r.get("status") != "ok":
            continue
        region = r["region"]
        mape   = r["mape"]
        mae    = r["mae"]
        cal    = r["ci_verdict"]

        if mape > 0.30 and mae < 1000:
            log.info(
                f"  {region}: High MAPE={mape:.1%} but MAE={mae:.0f} deaths — "
                f"small absolute error, high % because region has few deaths."
            )
        elif mape > 0.30:
            log.info(
                f"  {region}: High MAPE={mape:.1%} and MAE={mae:,.0f} deaths — "
                f"genuinely difficult to forecast. Likely COVID disruption."
            )
        if cal != "well_calibrated":
            log.warning(
                f"  {region}: CI is {cal} (coverage={r['ci_coverage']:.0%}). "
                f"Analysts should treat uncertainty bands with caution."
            )

    csv_path = "/tmp/prophet_validation.csv"
    summary_df.to_csv(csv_path, index=False)

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("malaria_stage5_validation")

    with mlflow.start_run(run_name="prophet_validation"):

        for r in results:
            if r.get("status") != "ok":
                continue
            region = r["region"]
            mlflow.log_metric(f"prophet_mape_{region}",        r["mape"])
            mlflow.log_metric(f"prophet_mae_{region}",         r["mae"])
            mlflow.log_metric(f"prophet_ci_coverage_{region}", r["ci_coverage"])
            mlflow.log_param(f"prophet_ci_verdict_{region}",   r["ci_verdict"])

        ok_results = [r for r in results if r.get("status") == "ok"]
        if ok_results:
            mean_coverage = np.mean([r["ci_coverage"] for r in ok_results])
            mlflow.log_metric("prophet_mean_ci_coverage", round(mean_coverage, 3))

        # Artifacts
        if accuracy_plot:
            mlflow.log_artifact(accuracy_plot,    artifact_path="prophet_plots")
        if calibration_plot:
            mlflow.log_artifact(calibration_plot, artifact_path="prophet_plots")
        for p in region_plots:
            mlflow.log_artifact(p, artifact_path="prophet_plots/regions")
        mlflow.log_artifact(csv_path, artifact_path="prophet_data")

        log.info(f"\nAll results logged to MLflow experiment: malaria_stage5_validation")

    log.info("\nProphet validation complete.")


if __name__ == "__main__":
    run_prophet_validation()