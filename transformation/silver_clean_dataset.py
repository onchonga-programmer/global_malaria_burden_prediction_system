import boto3
import pandas as pd
import io
import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


DATASET_CONFIGS = {
    "malaria_deaths": {
        "bronze_prefix": "bronze/malaria/malaria_deaths_who_",
        "silver_key": "silver/malaria/deaths.parquet",
        "rename": {
            "entity": "country",
            "year": "year",
            "death_count__age_group_allages__sex_both_sexes__cause_malaria": "deaths",
        },
    },
    "malaria_death_rate": {
        "bronze_prefix": "bronze/malaria/malaria_death_rate_",
        "silver_key": "silver/malaria/death_rate.parquet",
        "rename": {
            "entity": "country",
            "year": "year",
            "death_rate100k__age_group_allages__sex_both_sexes__cause_malaria": "death_rate_per_100k",
        },
    },
    "malaria_incidence": {
        "bronze_prefix": "bronze/malaria/malaria_incidence_",
        "silver_key": "silver/malaria/incidence.parquet",
        "rename": {
            "entity": "country",
            "year": "year",
            "sh_mlr_incd_p3": "incidence_per_1000",
        },
    },
}

def get_latest_bronze_key(s3_client, bucket: str, prefix: str) -> str:
   
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if "Contents" not in response:
        raise FileNotFoundError(f"No bronze files found with prefix: {prefix}")
    
    # Sort by LastModified to get the truly latest file
    objects = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    latest_key = objects[0]["Key"]
    logger.info(f"Latest bronze file: {latest_key}")
    return latest_key


def read_bronze_json(s3_client, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw = json.loads(obj["Body"].read().decode("utf-8"))
    
    columns = raw["columns"]  
    rows = raw["data"]        
    
    df = pd.DataFrame(rows, columns=columns)
    logger.info(f"Read {len(df)} rows, columns: {columns}")
    return df

def clean_dataset(df: pd.DataFrame, config: dict) -> pd.DataFrame:
  
    # 1. Lowercase all column names, replace spaces with underscores
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    
    # 2. Rename columns to our standard names
    rename_map = {k.lower(): v for k, v in config["rename"].items()}
    df = df.rename(columns=rename_map)
    
    # 3. Keep only the columns we renamed (drop anything else)
    keep_cols = list(config["rename"].values())
    # Only keep columns that actually exist after rename
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]
    
    # 4. Type casting
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["country"] = df["country"].astype(str).str.strip()
    
    # Cast the value column (whatever it got renamed to)
    value_col = [c for c in keep_cols if c not in ("country", "year")][0]
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    
    # 5. Drop rows where year or the value column is null
    before = len(df)
    df = df.dropna(subset=["year", value_col])
    after = len(df)
    if before != after:
        logger.warning(f"Dropped {before - after} rows with null year or {value_col}")
    
    
    aggregate_regions = {
        "World", "Africa", "Asia", "Europe", "Americas",
        "North America", "South America", "Oceania",
        "Sub-Saharan Africa", "South Asia", "East Asia & Pacific",
        "High-income countries", "Low-income countries",
        "Lower-middle-income countries", "Upper-middle-income countries"
    }
    df["is_aggregate_region"] = df["country"].isin(aggregate_regions)
    
    # 7. Sort for consistency
    df = df.sort_values(["country", "year"]).reset_index(drop=True)
    
    logger.info(f"Cleaned dataset: {len(df)} rows remaining")
    return df


def write_parquet_to_s3(df: pd.DataFrame, s3_client, bucket: str, key: str):
  
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)  
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(f"Written to s3://{bucket}/{key} ({len(df)} rows)")


def run_silver_cleaning(bucket: str, s3_client=None):
   
    if s3_client is None:
        s3_client = boto3.client("s3")
    
    cleaned = {}
    
    for name, config in DATASET_CONFIGS.items():
        logger.info(f"Processing silver layer for: {name}")
        try:
            bronze_key = get_latest_bronze_key(s3_client, bucket, config["bronze_prefix"])
            df = read_bronze_json(s3_client, bucket, bronze_key)
            df_clean = clean_dataset(df, config)
            write_parquet_to_s3(df_clean, s3_client, bucket, config["silver_key"])
            cleaned[name] = df_clean
            logger.info(f"✓ {name}: silver layer complete")
        except Exception as e:
            logger.error(f"✗ {name} failed: {e}")
            raise
    
    return cleaned