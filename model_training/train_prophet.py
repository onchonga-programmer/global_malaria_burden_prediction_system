# stages/stage4/train_prophet.py

import mlflow
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os
import sys
import warnings
warnings.filterwarnings("ignore")   

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
FORECAST_YEARS = 3   


def train_prophet_for_region(region_name: str, region_df: pd.DataFrame) -> dict:
  
    print(f"\n── Training Prophet for {region_name} ──")
    print(f"   Data: {len(region_df)} years  |  "
          f"deaths range {region_df['y'].min():,.0f} – {region_df['y'].max():,.0f}")

   
    region_df = region_df[region_df["y"] > 0].copy()
    print(f"   After removing zero-death years: {len(region_df)} years")

    model = Prophet(
        changepoint_prior_scale=0.3,
        seasonality_mode="additive",
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=0.95,   )
    model.fit(region_df)

   
    future = model.make_future_dataframe(periods=FORECAST_YEARS, freq="YS")

    forecast = model.predict(future)

    last_training_year = region_df["ds"].dt.year.max()
    future_forecast = forecast[
        forecast["ds"].dt.year > last_training_year
    ][["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()

    # Clip negative predictions to zero
    future_forecast["yhat"]       = future_forecast["yhat"].clip(lower=0)
    future_forecast["yhat_lower"] = future_forecast["yhat_lower"].clip(lower=0)
    future_forecast["yhat_upper"] = future_forecast["yhat_upper"].clip(lower=0)

    print(f"   Forecasts:")
    for _, row in future_forecast.iterrows():
        year = row["ds"].year
        print(f"     {year}: {row['yhat']:>10,.0f} deaths  "
              f"(range {row['yhat_lower']:,.0f} – {row['yhat_upper']:,.0f})")

   
    metrics = {}
    try:
        print(f"   Running cross validation...")
        df_cv = cross_validation(
            model,
            initial="3650 days",   
            period="365 days",     
            horizon="730 days",    
            disable_tqdm=True,
        )
        df_metrics = performance_metrics(df_cv)

        mae  = df_metrics["mae"].mean()
        rmse = df_metrics["rmse"].mean()
        mape = df_metrics["mape"].mean()

        metrics = {"mae": mae, "rmse": rmse, "mape": mape}
        print(f"   MAE:  {mae:>12,.0f} deaths average error")
        print(f"   RMSE: {rmse:>12,.0f}")
        print(f"   MAPE: {mape:>12.1%}  (% error relative to actual)")

    except Exception as e:
        print(f"   Cross validation skipped: {e}")
        metrics = {"mae": None, "rmse": None, "mape": None}

    return model, forecast, future_forecast, metrics


def save_forecast_plot(region_name: str, model, forecast, region_df) -> str:
    """
    Save a visual forecast plot showing:
    - Black dots: actual historical deaths
    - Blue line: model's fitted trend + forecast
    - Light blue band: 95% uncertainty interval
    """
    fig = model.plot(forecast, figsize=(12, 5))
    plt.title(f"Malaria Death Forecast — {region_name}", fontsize=14)
    plt.xlabel("Year")
    plt.ylabel("Deaths")
    plt.tight_layout()

    path = f"/tmp/prophet_forecast_{region_name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def save_components_plot(region_name: str, model, forecast) -> str:
    """
    Save a components plot showing trend and any seasonal patterns separately.
    This is the 'explainability' plot — shows WHY the model forecasts what it does.
    """
    fig = model.plot_components(forecast, figsize=(12, 6))
    plt.tight_layout()
    path = f"/tmp/prophet_components_{region_name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def train_all_regions(prophet_dfs: dict):
    """
    Loop through all regions, train one Prophet model each,
    log everything to MLflow under a separate experiment per region.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    all_forecasts = {}  

    for region_name, region_df in prophet_dfs.items():

        experiment_name = f"malaria_prophet_{region_name}"
        mlflow.set_experiment(experiment_name)

        with mlflow.start_run(run_name=f"prophet_{region_name}_v1"):

            # ── Train ─────────────────────────────────────────────────────────
            model, forecast, future_forecast, metrics = \
                train_prophet_for_region(region_name, region_df)

            # ── Log parameters ────────────────────────────────────────────────
            mlflow.log_params({
                "region":                  region_name,
                "changepoint_prior_scale": 0.3,
                "forecast_years":          FORECAST_YEARS,
                "training_years":          len(region_df),
                "interval_width":          0.95,
            })

            # ── Log metrics ───────────────────────────────────────────────────
            clean_metrics = {k: v for k, v in metrics.items() if v is not None}
            if clean_metrics:
                mlflow.log_metrics(clean_metrics)

            # ── Log forecast table as CSV artifact ────────────────────────────
            csv_path = f"/tmp/forecast_{region_name}.csv"
            future_forecast.to_csv(csv_path, index=False)
            mlflow.log_artifact(csv_path, artifact_path="forecasts")

            # ── Log plots ─────────────────────────────────────────────────────
            forecast_plot_path   = save_forecast_plot(region_name, model, forecast, region_df)
            components_plot_path = save_components_plot(region_name, model, forecast)
            mlflow.log_artifact(forecast_plot_path,   artifact_path="plots")
            mlflow.log_artifact(components_plot_path, artifact_path="plots")

            # ── Save model ────────────────────────────────────────────────────
            mlflow.prophet.log_model(
                model,
                artifact_path="model",
                registered_model_name=f"malaria_prophet_{region_name}",
            )

            run_id = mlflow.active_run().info.run_id
            print(f"   ✅ Logged to MLflow — run_id: {run_id[:8]}...")

            all_forecasts[region_name] = future_forecast

    # ── Save combined forecast across all regions ─────────────────────────────
    combined = pd.concat(
        [df.assign(region=region) for region, df in all_forecasts.items()],
        ignore_index=True
    )
    combined_path = "/tmp/all_regions_forecast.csv"
    combined.to_csv(combined_path, index=False)
    print(f"\n── Combined forecast saved ──")
    print(combined.to_string(index=False))

    return all_forecasts


if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from model_training.data_prep import load_gold_from_s3, prepare_prophet_data

    df = load_gold_from_s3()
    prophet_dfs = prepare_prophet_data(df)
    train_all_regions(prophet_dfs)