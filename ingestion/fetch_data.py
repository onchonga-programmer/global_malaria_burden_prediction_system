# ingestion/fetch_owid.py

import requests
import json
import os
import time
from datetime import datetime, timezone
import logging
from logging.handlers import RotatingFileHandler


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("malaria_pipeline")
logger.setLevel(logging.INFO)

# Handler 1 — terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Handler 2 — file
file_handler = RotatingFileHandler(
    filename=os.path.join(LOG_DIR, "ingestion.log"),
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
    encoding="utf-8",
)
file_handler.setLevel(logging.INFO)

# Same format for both
formatter = logging.Formatter(
    "%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# ── Dataset URLs ──────────────────────────────────────────────────────────────

DATASETS = {
    "malaria_death_rate": (
        "https://ourworldindata.org/grapher/death-rate-from-malaria-ghe.csv"
        "?v=1&csvType=full&useColumnShortNames=true"
    ),
    "malaria_incidence": (
        "https://ourworldindata.org/grapher/incidence-of-malaria.csv"
        "?v=1&csvType=full&useColumnShortNames=true"
    ),
    "malaria_deaths_who": (
        "https://ourworldindata.org/grapher/number-of-deaths-from-malaria-ghe.csv"
        "?v=1&csvType=full&useColumnShortNames=true"
    ),
   
}
RAW_DATA_DIR = "data/raw"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; MalariaForecastPipeline/1.0; "
        "research project; contact: your@email.com)"
    ),
    "Accept": "text/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://ourworldindata.org/",
}


def fetch_csv_as_json(url: str, dataset_name: str) -> dict:
    logger.info(f"Fetching: {dataset_name}")

    try:
        time.sleep(1)
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error for {dataset_name}: {e}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error for {dataset_name}: {e}")
        raise

    lines = response.text.strip().split("\n")
    headers = [h.strip().strip('"') for h in lines[0].split(",")]

    records = []
    for line in lines[1:]:
        values = [v.strip().strip('"') for v in line.split(",")]
        if len(values) == len(headers):
            records.append(dict(zip(headers, values)))

    logger.info(f"  → {len(records)} records fetched")

    return {
        "dataset_name": dataset_name,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source_url": url,
        "record_count": len(records),
        "columns": headers,
        "data": records,
    }


def save_locally(payload: dict, filename: str) -> str:
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    filepath = os.path.join(RAW_DATA_DIR, filename)

    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)

    logger.info(f"  → Saved: {filepath}")
    return filepath


def ingest_malaria_data() -> list:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    saved_files = []
    failed = []

    logger.info("=" * 50)
    logger.info("Starting malaria data ingestion")
    logger.info("=" * 50)

    for name, url in DATASETS.items():
        try:
            payload = fetch_csv_as_json(url, name)
            filename = f"{name}_{timestamp}.json"
            filepath = save_locally(payload, filename)
            saved_files.append(filepath)
        except Exception as e:
            logger.error(f"Skipping {name}: {e}")
            failed.append(name)

    logger.info("=" * 50)
    logger.info(f"Ingestion complete: {len(saved_files)}/{len(DATASETS)} succeeded")
    if failed:
        logger.warning(f"Failed datasets: {failed}")
    logger.info("=" * 50)

    return saved_files


if __name__ == "__main__":
    files = ingest_malaria_data()
    for f in files:
        logger.info(f"  ✓ {f}")