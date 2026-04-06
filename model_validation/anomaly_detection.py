

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
import matplotlib.cm as cm
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────

S3_BUCKET  = os.getenv("S3_BUCKET", "malaria-forecast-bree")
GOLD_KEY   = "gold/features.parquet"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

WHO_REGIONS = ["AFRO", "AMRO", "EMRO", "SEARO", "WPRO"]

# Features we use to characterise each country's trajectory.
# These describe the shape of a country's malaria burden over time,
# not a single year — we aggregate to one row per country first.
TRAJECTORY_FEATURES = [
    "deaths_mean",           # average annual deaths (scale of burden)
    "deaths_trend",          # slope of deaths over time (improving/worsening)
    "deaths_volatility",     # std dev of annual deaths (how stable)
    "death_rate_mean",       # average death rate per 100k
    "death_rate_trend",      # slope of death rate over time
    "pct_years_improving",   # fraction of years where deaths fell
    "yoy_mean",              # mean year-over-year % change
    "yoy_volatility",        # std dev of year-over-year % change
]

# Isolation Forest contamination parameter.
# This tells the model roughly what fraction of points to flag as anomalies.
# 0.10 = flag the most extreme ~10% of countries per region.
# Adjust upward if you want more flags, downward for stricter filtering.
CONTAMINATION = 0.10

# Analysis window — we characterise trajectories over recent years only
TRAJECTORY_START = 2010
TRAJECTORY_END   = 2022


# ── Data loading and feature engineering ──────────────────────────────────────

def load_gold_data() -> pd.DataFrame:
    log.info("Loading gold data from S3...")
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    log.info(f"  Loaded {len(df):,} rows")
    return df


def build_country_trajectories(df: pd.DataFrame) -> pd.DataFrame:
  
    log.info("Building country trajectory features...")

    # Filter to country-level rows (not regional aggregates) and time window
    mask = (
        (df["is_aggregate_region"] == False) &
        (df["year"] >= TRAJECTORY_START) &
        (df["year"] <= TRAJECTORY_END)
    )
    df = df[mask].copy()

    rows = []
    for (country, region), grp in df.groupby(["country", "who_region"]):
        grp = grp.sort_values("year")

        # Need at least 5 years to compute a meaningful trajectory
        if len(grp) < 5 or grp["deaths"].isna().all():
            continue

        deaths      = grp["deaths"].dropna()
        death_rate  = grp["death_rate_per_100k"].dropna()
        yoy         = grp["deaths_yoy_pct"].dropna()
        years       = grp.loc[deaths.index, "year"].values

        # Linear trend — slope of a regression line through deaths over time
        # Positive slope = deaths rising (worsening)
        # Negative slope = deaths falling (improving)
        if len(years) >= 3:
            deaths_trend = float(np.polyfit(years, deaths.values, 1)[0])
        else:
            deaths_trend = float(deaths.diff().mean())

        if len(death_rate) >= 3:
            dr_years    = grp.loc[death_rate.index, "year"].values
            rate_trend  = float(np.polyfit(dr_years, death_rate.values, 1)[0])
        else:
            rate_trend  = float(death_rate.diff().mean()) if len(death_rate) > 1 else 0.0

        # Fraction of years where deaths fell compared to previous year
        pct_improving = float((grp["improving"] == 1).mean()) if "improving" in grp.columns else float(np.nan)

        rows.append({
            "country":            country,
            "who_region":         region,
            "n_years":            len(grp),
            "deaths_mean":        float(deaths.mean()),
            "deaths_trend":       deaths_trend,
            "deaths_volatility":  float(deaths.std()),
            "death_rate_mean":    float(death_rate.mean()) if len(death_rate) > 0 else np.nan,
            "death_rate_trend":   rate_trend,
            "pct_years_improving": pct_improving,
            "yoy_mean":           float(yoy.mean())        if len(yoy) > 0 else np.nan,
            "yoy_volatility":     float(yoy.std())         if len(yoy) > 0 else np.nan,
        })

    trajectory_df = pd.DataFrame(rows)
    log.info(f"  Built trajectories for {len(trajectory_df)} countries")
    return trajectory_df


# ── Isolation Forest per region ────────────────────────────────────────────────

def detect_anomalies_for_region(
    region_df: pd.DataFrame,
    region: str,
) -> pd.DataFrame:
  
    # Drop rows with any NaN in our feature set
    feature_mask = [f for f in TRAJECTORY_FEATURES if f in region_df.columns]
    region_df    = region_df.dropna(subset=feature_mask).copy()

    if len(region_df) < 5:
        log.warning(f"  {region}: only {len(region_df)} countries with complete features — skipping")
        return pd.DataFrame()

    X = region_df[feature_mask].values

    # Scale features: essential step before any distance-based algorithm
    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Fit Isolation Forest
    # contamination = expected fraction of anomalies in the dataset
    # random_state  = reproducibility
    iso = IsolationForest(
        contamination=CONTAMINATION,
        random_state=42,
        n_estimators=200,
    )
    iso.fit(X_scaled)

    # score_samples returns negative scores — more negative = more anomalous
    # predict returns +1 (normal) or -1 (anomaly)
    region_df["anomaly_score"] = iso.score_samples(X_scaled)
    region_df["is_anomaly"]    = iso.predict(X_scaled) == -1

    # Classify direction of anomaly
    # If a flagged country has negative deaths_trend → improving faster than peers
    # If positive deaths_trend → deteriorating faster than peers
    def classify_direction(row):
        if not row["is_anomaly"]:
            return "normal"
        # Use death_rate_trend if available (more meaningful than raw deaths)
        trend_col = "death_rate_trend" if "death_rate_trend" in row.index else "deaths_trend"
        return "positive" if row[trend_col] < 0 else "negative"

    region_df["anomaly_type"] = region_df.apply(classify_direction, axis=1)

    n_flagged = region_df["is_anomaly"].sum()
    log.info(f"  {region}: {len(region_df)} countries, {n_flagged} flagged")

    flagged = region_df[region_df["is_anomaly"]].sort_values("anomaly_score")
    for _, row in flagged.iterrows():
        direction = "↑ POSITIVE" if row["anomaly_type"] == "positive" else "↓ NEGATIVE"
        log.info(
            f"    {direction}  {row['country']:<30} "
            f"score={row['anomaly_score']:.3f}  "
            f"trend={row['deaths_trend']:+.0f} deaths/yr"
        )

    return region_df


# ── Plotting ───────────────────────────────────────────────────────────────────

def plot_anomaly_scatter(all_df: pd.DataFrame) -> str:
    
    regions   = [r for r in WHO_REGIONS if r in all_df["who_region"].values]
    n_regions = len(regions)

    fig, axes = plt.subplots(1, n_regions, figsize=(4 * n_regions, 5))
    if n_regions == 1:
        axes = [axes]

    for ax, region in zip(axes, regions):
        r_df = all_df[all_df["who_region"] == region]
        if r_df.empty:
            continue

        normal   = r_df[~r_df["is_anomaly"]]
        positive = r_df[(r_df["is_anomaly"]) & (r_df["anomaly_type"] == "positive")]
        negative = r_df[(r_df["is_anomaly"]) & (r_df["anomaly_type"] == "negative")]

        ax.scatter(normal["deaths_trend"],   normal["deaths_volatility"],
                   c="#888", alpha=0.5, s=35, label="Normal")
        ax.scatter(positive["deaths_trend"], positive["deaths_volatility"],
                   c="#59A14F", marker="^", s=90, zorder=5, label="Positive anomaly")
        ax.scatter(negative["deaths_trend"], negative["deaths_volatility"],
                   c="#E15759", marker="v", s=90, zorder=5, label="Negative anomaly")

        # Label the flagged countries
        for _, row in pd.concat([positive, negative]).iterrows():
            ax.annotate(
                row["country"],
                xy=(row["deaths_trend"], row["deaths_volatility"]),
                xytext=(5, 5), textcoords="offset points",
                fontsize=7, alpha=0.85
            )

        ax.axvline(0, color="gray", linewidth=0.6, linestyle="--", alpha=0.5)
        ax.set_title(region, fontsize=11)
        ax.set_xlabel("Deaths trend\n(negative = improving)", fontsize=8)
        ax.set_ylabel("Deaths volatility" if ax == axes[0] else "", fontsize=8)
        ax.tick_params(labelsize=8)

    # Single legend for the whole figure
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, fontsize=9, framealpha=0.8)
    fig.suptitle(
        "Anomaly detection — country trajectories within each WHO region\n"
        f"Period: {TRAJECTORY_START}–{TRAJECTORY_END}  |  "
        f"Flagged: top {int(CONTAMINATION*100)}% most outlying countries",
        fontsize=11
    )
    fig.tight_layout(rect=[0, 0.08, 1, 1])

    path = "/tmp/anomaly_scatter.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


def plot_anomaly_trajectories(df_raw: pd.DataFrame, all_summary: pd.DataFrame) -> str:
 
    flagged = all_summary[all_summary["is_anomaly"]].copy()
    if flagged.empty:
        return None

    # Limit to top 8 most anomalous countries for readability
    flagged = flagged.nsmallest(8, "anomaly_score")

    # Get country-level raw time series
    mask = (
        (df_raw["is_aggregate_region"] == False) &
        (df_raw["year"] >= TRAJECTORY_START) &
        (df_raw["year"] <= TRAJECTORY_END)
    )
    ts_df = df_raw[mask].copy()

    n_plots = len(flagged)
    ncols   = 4
    nrows   = int(np.ceil(n_plots / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows))
    axes = axes.flatten()

    for i, (_, row) in enumerate(flagged.iterrows()):
        ax      = axes[i]
        country = row["country"]
        region  = row["who_region"]
        atype   = row["anomaly_type"]

        # Country time series
        country_ts = (
            ts_df[ts_df["country"] == country][["year", "deaths"]]
            .dropna().sort_values("year")
        )

        # Regional median across all countries in that region
        region_ts = (
            ts_df[ts_df["who_region"] == region]
            .groupby("year")["deaths"]
            .median()
            .reset_index()
        )

        color = "#59A14F" if atype == "positive" else "#E15759"

        ax.plot(region_ts["year"],   region_ts["deaths"],
                color="#aaa", linewidth=1.5, linestyle="--", label="Regional median")
        ax.plot(country_ts["year"],  country_ts["deaths"],
                color=color,  linewidth=2,   label=country)

        direction = "↑ improving faster" if atype == "positive" else "↓ deteriorating faster"
        ax.set_title(f"{country}\n{direction}", fontsize=9, color=color)
        ax.set_xlabel("Year", fontsize=8)
        ax.set_ylabel("Deaths", fontsize=8)
        ax.tick_params(labelsize=7)
        ax.legend(fontsize=7)

    # Hide unused subplots
    for j in range(n_plots, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(
        "Anomalous country trajectories vs regional median\n"
        "Green = improving faster than peers  |  Red = deteriorating faster than peers",
        fontsize=11
    )
    fig.tight_layout()

    path = "/tmp/anomaly_trajectories.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return path


# ── Main ───────────────────────────────────────────────────────────────────────

def run_anomaly_detection():
    log.info("=" * 60)
    log.info("Stage 5 — Anomaly Detection")
    log.info("=" * 60)

    # ── Load and prepare ──
    df_raw       = load_gold_data()
    trajectory_df = build_country_trajectories(df_raw)

    # ── Detect anomalies per region ──
    region_results = []
    for region in WHO_REGIONS:
        region_df = trajectory_df[trajectory_df["who_region"] == region].copy()
        if region_df.empty:
            continue
        result_df = detect_anomalies_for_region(region_df, region)
        if not result_df.empty:
            region_results.append(result_df)

    if not region_results:
        log.error("No anomaly results produced — check data.")
        return

    all_summary = pd.concat(region_results, ignore_index=True)

    # ── Summary statistics ──
    total_countries = len(all_summary)
    total_flagged   = all_summary["is_anomaly"].sum()
    positive_flags  = (all_summary["anomaly_type"] == "positive").sum()
    negative_flags  = (all_summary["anomaly_type"] == "negative").sum()

    log.info(f"\nOverall summary:")
    log.info(f"  Countries analysed: {total_countries}")
    log.info(f"  Flagged as anomalies: {total_flagged}")
    log.info(f"    Positive (improving faster than peers): {positive_flags}")
    log.info(f"    Negative (deteriorating faster than peers): {negative_flags}")

    # ── Plots ──
    scatter_path = plot_anomaly_scatter(all_summary)
    traj_path    = plot_anomaly_trajectories(df_raw, all_summary)

    # ── Save results CSV ──
    cols_to_save = [
        "country", "who_region", "n_years",
        "deaths_mean", "deaths_trend", "deaths_volatility",
        "death_rate_mean", "death_rate_trend",
        "pct_years_improving", "yoy_mean", "yoy_volatility",
        "anomaly_score", "is_anomaly", "anomaly_type",
    ]
    cols_to_save = [c for c in cols_to_save if c in all_summary.columns]
    csv_path = "/tmp/anomaly_results.csv"
    all_summary[cols_to_save].sort_values("anomaly_score").to_csv(csv_path, index=False)

    # ── Log to MLflow ──
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("malaria_stage5_validation")

    with mlflow.start_run(run_name="anomaly_detection"):

        mlflow.log_metric("anomaly_total_countries", total_countries)
        mlflow.log_metric("anomaly_total_flagged",   int(total_flagged))
        mlflow.log_metric("anomaly_positive_flags",  int(positive_flags))
        mlflow.log_metric("anomaly_negative_flags",  int(negative_flags))
        mlflow.log_param("anomaly_contamination",    CONTAMINATION)
        mlflow.log_param("anomaly_period",           f"{TRAJECTORY_START}-{TRAJECTORY_END}")

        # Log each flagged country as a param for easy visibility in MLflow UI
        flagged_list = (
            all_summary[all_summary["is_anomaly"]][["country", "anomaly_type", "who_region"]]
            .sort_values("who_region")
            .apply(lambda r: f"{r['country']} ({r['who_region']}, {r['anomaly_type']})", axis=1)
            .tolist()
        )
        mlflow.log_param("flagged_countries", "; ".join(flagged_list[:20]))  # MLflow param limit

        # Per-region counts
        for region in WHO_REGIONS:
            r_df = all_summary[all_summary["who_region"] == region]
            if r_df.empty:
                continue
            mlflow.log_metric(f"anomaly_flagged_{region}", int(r_df["is_anomaly"].sum()))

        # Artifacts
        mlflow.log_artifact(scatter_path, artifact_path="anomaly_plots")
        if traj_path:
            mlflow.log_artifact(traj_path, artifact_path="anomaly_plots")
        mlflow.log_artifact(csv_path, artifact_path="anomaly_data")

        log.info(f"\nAll results logged to MLflow experiment: malaria_stage5_validation")

    log.info("\nAnomaly detection complete.")


if __name__ == "__main__":
    run_anomaly_detection()