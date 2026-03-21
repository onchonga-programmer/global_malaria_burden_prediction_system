import boto3
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)

GOLD_KEY = "gold/malaria_features/features.parquet"


def run_gold_features(cleaned_datasets: dict, bucket: str, s3_client=None):
    """
    Join silver datasets and engineer ML features.
    
    cleaned_datasets: dict from run_silver_cleaning() 
                      {"malaria_deaths": df, "malaria_death_rate": df, ...}
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    
    deaths = cleaned_datasets["malaria_deaths"]
    death_rate = cleaned_datasets["malaria_death_rate"]
    incidence = cleaned_datasets["malaria_incidence"]
    
   
    df = deaths.merge(death_rate, on=["country", "year", "is_aggregate_region"], how="outer")
    df = df.merge(incidence, on=["country", "year", "is_aggregate_region"], how="outer")
    
    logger.info(f"After join: {len(df)} rows, {df['country'].nunique()} countries")
    
  
    df = df.sort_values(["country", "year"]).reset_index(drop=True)
    
   
    grp = df.groupby("country")
    
    for col in ["death_rate_per_100k", "deaths", "incidence_per_1000"]:
        if col in df.columns:
            df[f"{col}_lag1"] = grp[col].shift(1)
            df[f"{col}_lag2"] = grp[col].shift(2)
            df[f"{col}_lag3"] = grp[col].shift(3)
    
    for col in ["death_rate_per_100k", "deaths"]:
        if col in df.columns:
            df[f"{col}_rolling3"] = (
                grp[col]
                .transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean())
            )
    
    # Year-over-year percentage change
    for col in ["death_rate_per_100k", "deaths"]:
        if col in df.columns:
            df[f"{col}_yoy_pct"] = grp[col].pct_change() * 100
    
    who_regions = {
        "Nigeria": "AFRO", "Kenya": "AFRO", "Tanzania": "AFRO",
        "Uganda": "AFRO", "Ghana": "AFRO", "Mozambique": "AFRO",
        "India": "SEARO", "Bangladesh": "SEARO", "Myanmar": "SEARO",
        "Indonesia": "SEARO", "Thailand": "SEARO",
        "Brazil": "AMRO", "Colombia": "AMRO", "Venezuela": "AMRO",
        "Papua New Guinea": "WPRO", "Solomon Islands": "WPRO",
        "Sudan": "EMRO", "Somalia": "EMRO", "Yemen": "EMRO",
    }
    df["who_region"] = df["country"].map(who_regions).fillna("OTHER")
    
    # ── Year as a feature ─────────────────────────────────────────────────────
    df["year_normalized"] = (df["year"] - df["year"].min()) / (df["year"].max() - df["year"].min())
    
    df["improving"] = (df["death_rate_per_100k_yoy_pct"] < 0).astype("Int64")
    
    logger.info(f"Feature engineering complete. Columns: {list(df.columns)}")
    
    # ── Write to gold ─────────────────────────────────────────────────────────
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket,
        Key=GOLD_KEY,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    
    logger.info(f"Gold layer written: s3://{bucket}/{GOLD_KEY}")
    return df