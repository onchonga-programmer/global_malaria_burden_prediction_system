import os
import sys
import logging
import boto3
from dotenv import load_dotenv
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/transformation.log"),
    ],
)
logger = logging.getLogger("run_transformation")

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv()
BUCKET = os.getenv("S3_BUCKET_NAME")

if not BUCKET:
    logger.error("S3_BUCKET_NAME not set in .env")
    sys.exit(1)

# ── Import our modules ────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from silver_clean_dataset import run_silver_cleaning
from gold_build_features import run_gold_features


def main():
    logger.info("=" * 60)
    logger.info("Stage 2 — Transformation pipeline starting")
    logger.info(f"Target bucket: {BUCKET}")
    logger.info("=" * 60)
    
    s3_client = boto3.client("s3")
    
    # Step 1: Silver layer
    logger.info("--- Silver layer ---")
    cleaned = run_silver_cleaning(bucket=BUCKET, s3_client=s3_client)
    logger.info("Silver layer complete ✓")
    
    # Step 2: Gold layer
    logger.info("--- Gold layer ---")
    gold_df = run_gold_features(cleaned_datasets=cleaned, bucket=BUCKET, s3_client=s3_client)
    logger.info("Gold layer complete ✓")
    
    # Step 3: Summary
    logger.info("=" * 60)
    logger.info("Transformation complete.")
    logger.info(f"Gold table shape: {gold_df.shape}")
    logger.info(f"Countries: {gold_df['country'].nunique()}")
    logger.info(f"Year range: {gold_df['year'].min()} – {gold_df['year'].max()}")
    logger.info("=" * 60)
    sys.exit(0)


if __name__ == "__main__":
    main()