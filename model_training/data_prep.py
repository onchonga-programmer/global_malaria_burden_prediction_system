
import boto3
import pandas as pd
import numpy as np
import io
import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "malaria-forecast-bree")
GOLD_KEY  = "gold/malaria_features/features.parquet"


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

REGION_MAP = {
    "AFRO":  0,
    "SEARO": 1,
    "EMRO":  2,
    "WPRO":  3,
    "AMRO":  4,
    "EURO":  5,
    "Other": 6,
}


def load_gold_from_s3() -> pd.DataFrame:
    print(f"Loading gold data from s3://{S3_BUCKET}/{GOLD_KEY}")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    print(f"  Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


def drop_aggregate_regions(df: pd.DataFrame) -> pd.DataFrame:
  
    before = len(df)
    df = df[df["is_aggregate_region"] == False].copy()
    print(f"  Dropped {before - len(df)} aggregate region rows "
          f"({df['country'].nunique()} real countries remain)")
    return df


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add features not already in the gold layer."""
    df["is_covid_period"] = (df["year"] >= 2020).astype(int)
    # encode who_region string → integer for LightGBM
    df["who_region_encoded"] = df["who_region"].map(REGION_MAP).fillna(6).astype(int)
    return df


def filter_complete_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Keep countries with deaths data in at least 18 of the available years."""
    country_completeness = (
        df.groupby("country")["deaths"]
        .apply(lambda g: g.notna().sum())
    )
    complete_countries = country_completeness[country_completeness >= 18].index
    df_complete = df[df["country"].isin(complete_countries)].copy()
    print(f"  Complete countries: {len(complete_countries)} "
          f"(dropped {df['country'].nunique() - len(complete_countries)})")
    return df_complete


def time_aware_split(df: pd.DataFrame, cutoff_year: int = 2019):
   
    train = df[df["year"] < cutoff_year].copy()
    test  = df[df["year"] >= cutoff_year].copy()
    print(f"  Train: {len(train):,} rows  "
          f"(years {train['year'].min()}–{train['year'].max()})")
    print(f"  Test:  {len(test):,} rows   "
          f"(years {test['year'].min()}–{test['year'].max()})")
    return train, test


def compute_scale_pos_weight(train: pd.DataFrame) -> float:
    neg = (train[TARGET_COL] == 0).sum()
    pos = (train[TARGET_COL] == 1).sum()
    ratio = round(neg / pos, 2)
    print(f"  Class balance — not improving: {neg:,}  |  improving: {pos:,}")
    print(f"  scale_pos_weight = {ratio}")
    return ratio


def prepare_lgbm_data(df: pd.DataFrame):
   
    print("\n── Preparing LightGBM data ──")
    df = drop_aggregate_regions(df)
    df = add_engineered_features(df)
    df = filter_complete_countries(df)
    df = df.dropna(subset=[TARGET_COL])

    train, test = time_aware_split(df)
    spw = compute_scale_pos_weight(train)

    available_features = [c for c in FEATURE_COLS if c in train.columns]
    missing = set(FEATURE_COLS) - set(available_features)
    if missing:
        print(f"  WARNING: missing feature columns: {missing}")

    X_train = train[available_features]
    X_test  = test[available_features]
    y_train = train[TARGET_COL].astype(int)
    y_test  = test[TARGET_COL].astype(int)

    print(f"  Features used: {len(available_features)}")
    return X_train, X_test, y_train, y_test, spw


def prepare_prophet_data(df: pd.DataFrame) -> dict:
    
    print("\n── Preparing Prophet data ──")
    df = drop_aggregate_regions(df)

    prophet_dfs = {}
    for region_name in REGION_MAP:
        if region_name == "Other":
            continue

        region_df = (
            df[df["who_region"] == region_name]
            .groupby("year")["deaths"]
            .sum()
            .reset_index()
            .dropna()
        )

        if len(region_df) < 10:
            print(f"  Skipping {region_name} — only {len(region_df)} data points")
            continue

        region_df = region_df.rename(columns={"year": "ds", "deaths": "y"})
        region_df["ds"] = pd.to_datetime(region_df["ds"].astype(str) + "-01-01")

        prophet_dfs[region_name] = region_df
        print(f"  {region_name}: {len(region_df)} years  |  "
              f"deaths {region_df['y'].min():,.0f} – {region_df['y'].max():,.0f}")

    return prophet_dfs


if __name__ == "__main__":
    df = load_gold_from_s3()
    X_train, X_test, y_train, y_test, spw = prepare_lgbm_data(df)
    
    df2 = load_gold_from_s3()
    prophet_dfs = prepare_prophet_data(df2)

    print("\n── Final check ──")
    print(f"LightGBM X_train shape: {X_train.shape}")
    print(f"LightGBM X_test shape:  {X_test.shape}")
    print(f"Prophet regions ready:  {list(prophet_dfs.keys())}")