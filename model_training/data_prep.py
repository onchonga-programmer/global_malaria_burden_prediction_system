# stages/stage4/data_prep.py

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
    "incidence_per_1000",
    "deaths",
    "death_rate_per_100k",
    "lag1_deaths",
    "lag2_deaths",
    "lag3_deaths",
    "roll3_deaths",
    "yoy_pct_change_deaths",
    "who_region",
    "is_covid_period",   
]

TARGET_COL = "improving"

# ── WHO region encoding map
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
    """Pull the gold parquet file from S3 into a pandas DataFrame."""
    print(f"Loading gold data from s3://{S3_BUCKET}/{GOLD_KEY}")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    print(f"  Loaded {len(df):,} rows, {df.shape[1]} columns")
    return df


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add features that weren't in the gold layer."""

    df["is_covid_period"] = (df["year"] >= 2020).astype(int)

    return df


def filter_complete_countries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only countries with complete data for training.
    A country is 'complete' if it has non-null deaths AND
    non-null death_rate_per_100k for at least 18 of the 22 years.

    Why 18 not 22? A small tolerance for occasional missing years
    is more realistic than demanding perfection.
    """
    country_completeness = (
        df.groupby("country")[["deaths", "death_rate_per_100k"]]
        .apply(lambda g: g.notna().all(axis=1).sum())
    )
    complete_countries = country_completeness[country_completeness >= 18].index
    df_complete = df[df["country"].isin(complete_countries)].copy()

    print(f"  Complete countries: {len(complete_countries)} "
          f"(dropped {df['country'].nunique() - len(complete_countries)})")
    return df_complete


def encode_region(df: pd.DataFrame) -> pd.DataFrame:
    """Convert who_region string → integer for LightGBM."""
    df["who_region"] = df["who_region"].map(REGION_MAP).fillna(6).astype(int)
    return df


def time_aware_split(df: pd.DataFrame, cutoff_year: int = 2019):
    """
    Split into train and test sets based on time — NOT randomly.

    Train: all years before cutoff_year  (historical knowledge)
    Test:  cutoff_year and after         (held-out future)

    This is critical. Random splits would leak future data into training.
    """
    train = df[df["year"] < cutoff_year].copy()
    test  = df[df["year"] >= cutoff_year].copy()

    print(f"  Train: {len(train):,} rows  "
          f"(years {train['year'].min()}–{train['year'].max()})")
    print(f"  Test:  {len(test):,} rows   "
          f"(years {test['year'].min()}–{test['year'].max()})")
    return train, test


def compute_scale_pos_weight(train: pd.DataFrame) -> float:
    """
    LightGBM needs to know our class imbalance ratio.
    Formula: count of negatives / count of positives
    This tells the model: 'treat each positive example as if it were X negatives'
    """
    neg = (train[TARGET_COL] == 0).sum()
    pos = (train[TARGET_COL] == 1).sum()
    ratio = neg / pos
    print(f"  Class balance — not improving: {neg:,}  |  improving: {pos:,}")
    print(f"  scale_pos_weight = {ratio:.2f}")
    return ratio


def prepare_lgbm_data(df: pd.DataFrame):
    """
    Final preparation step for LightGBM.
    Returns X_train, X_test, y_train, y_test and the scale_pos_weight.
    """
    print("\n── Preparing LightGBM data ──")

    df = add_engineered_features(df)
    df = filter_complete_countries(df)
    df = encode_region(df)

    df = df.dropna(subset=[TARGET_COL])

    train, test = time_aware_split(df)
    spw = compute_scale_pos_weight(train)

    available_features = [c for c in FEATURE_COLS if c in train.columns]
    missing = set(FEATURE_COLS) - set(available_features)
    if missing:
        print(f"  WARNING: these feature columns not found: {missing}")

    X_train = train[available_features]
    X_test  = test[available_features]
    y_train = train[TARGET_COL].astype(int)
    y_test  = test[TARGET_COL].astype(int)

    return X_train, X_test, y_train, y_test, spw


def prepare_prophet_data(df: pd.DataFrame) -> dict:
    """
    Prophet needs a different shape: one DataFrame per WHO region,
    with columns exactly named 'ds' (date) and 'y' (value to forecast).

    Returns a dict: { region_name -> DataFrame with ds/y columns }
    """
    print("\n── Preparing Prophet data ──")

    prophet_dfs = {}

    for region_code, region_int in REGION_MAP.items():
        if region_code == "Other":
            continue

        region_df = (
            df[df["who_region_raw"] == region_code]   
            .groupby("year")["deaths"]
            .sum()
            .reset_index()
            .dropna()
        )

        if len(region_df) < 10:
            print(f"  Skipping {region_code} — only {len(region_df)} years of data")
            continue

        region_df = region_df.rename(columns={"year": "ds", "deaths": "y"})
        region_df["ds"] = pd.to_datetime(region_df["ds"].astype(str) + "-01-01")

        prophet_dfs[region_code] = region_df
        print(f"  {region_code}: {len(region_df)} years, "
              f"deaths range {region_df['y'].min():.0f}–{region_df['y'].max():.0f}")

    return prophet_dfs


if __name__ == "__main__":
    df = load_gold_from_s3()

    print("\n── Gold layer quick look ──")
    print(df.dtypes)
    print(f"\nColumns: {list(df.columns)}")
    print(f"Years:   {df['year'].min()}–{df['year'].max()}")
    print(f"Countries: {df['country'].nunique()}")
    print(f"\nTarget distribution:\n{df['improving'].value_counts(dropna=False)}")