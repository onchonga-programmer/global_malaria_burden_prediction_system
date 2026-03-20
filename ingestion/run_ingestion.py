

import sys
import os


from dotenv import load_dotenv
load_dotenv() 
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from fetch_data import ingest_malaria_data, logger
from upload_to_s3 import upload_all, verify_upload, get_s3_client


def run_stage_1():
    """
    Orchestrates the full Stage 1 pipeline:
    OWID API → local bronze → S3 bronze
    """
    logger.info("=" * 50)
    logger.info("STAGE 1 — Bronze Ingestion Pipeline")
    logger.info("=" * 50)

    # ── Step 1: Fetch from OWID ────────────────────────────────────────────
    logger.info("STEP 1/2 — Fetching data from OWID")
    local_files = ingest_malaria_data()

    if not local_files:
        logger.error("No files were fetched. Aborting pipeline.")
        sys.exit(1)  

    logger.info(f"  → {len(local_files)} file(s) saved locally")

    # ── Step 2: Upload to S3 ───────────────────────────────────────────────
    logger.info("STEP 2/2 — Uploading to S3 bronze layer")
    s3_paths = upload_all(local_files)

    if not s3_paths:
        logger.error("No files were uploaded to S3. Check AWS credentials.")
        sys.exit(1)

    # ── Step 3: Verify uploads ─────────────────────────────────────────────
    logger.info("Verifying uploads in S3...")
    s3_client = get_s3_client()
    all_verified = True

    for s3_path in s3_paths:
        verified = verify_upload(s3_path, s3_client)
        if not verified:
            all_verified = False

    # ── Summary ────────────────────────────────────────────────────────────
    logger.info("=" * 50)
    logger.info("STAGE 1 COMPLETE")
    logger.info(f"  Local files : {len(local_files)}")
    logger.info(f"  S3 uploads  : {len(s3_paths)}")
    logger.info(f"  Verified    : {'✓ All good' if all_verified else '✗ Some failed'}")
    logger.info("=" * 50)
    logger.info("S3 paths:")
    for path in s3_paths:
        logger.info(f"  {path}")
    logger.info("=" * 50)

    return s3_paths


if __name__ == "__main__":
    run_stage_1()