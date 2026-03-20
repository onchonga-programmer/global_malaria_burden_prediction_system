
import boto3
import os
import logging
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger("malaria_pipeline")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s"
    )


def get_s3_client():
    
    try:
        client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-2")
        )
        return client
    except NoCredentialsError:
        logger.error(
            "AWS credentials not found. "
            "Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in your .env"
        )
        raise


def upload_file_to_s3(local_path: str, s3_client) -> str:
    
    bucket = os.getenv("S3_BUCKET_NAME")

    if not bucket:
        raise ValueError(
            "S3_BUCKET_NAME not set. Check your .env file."
        )

    filename = Path(local_path).name
    s3_key = f"bronze/malaria/{filename}"

    logger.info(f"Uploading: {filename} → s3://{bucket}/{s3_key}")

    try:
        s3_client.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/json",
                "Metadata": {
                    "pipeline": "malaria-forecast",
                    "layer": "bronze",
                    "source": "owid",
                }
            }
        )
        s3_path = f"s3://{bucket}/{s3_key}"
        logger.info(f"  → Upload successful: {s3_path}")
        return s3_path

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error(f"S3 ClientError ({error_code}) for {filename}: {e}")
        raise

    except FileNotFoundError:
        logger.error(f"Local file not found: {local_path}")
        raise


def upload_all(file_paths: list) -> list:
   
    if not file_paths:
        logger.warning("No files to upload — file_paths list is empty")
        return []

    s3_client = get_s3_client()
    uploaded = []
    failed = []

    logger.info(f"Starting S3 upload — {len(file_paths)} file(s) to upload")

    for local_path in file_paths:
        try:
            s3_path = upload_file_to_s3(local_path, s3_client)
            uploaded.append(s3_path)
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            failed.append(local_path)

    logger.info(f"Upload complete: {len(uploaded)}/{len(file_paths)} succeeded")
    if failed:
        logger.warning(f"Failed uploads: {failed}")

    return uploaded


def verify_upload(s3_path: str, s3_client) -> bool:
    
    bucket = os.getenv("S3_BUCKET_NAME")
    key = s3_path.replace(f"s3://{bucket}/", "")

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"  ✓ Verified in S3: {key}")
        return True
    except ClientError:
        logger.error(f"  ✗ Verification failed — file not found in S3: {key}")
        return False


if __name__ == "__main__":
    from dotenv import load_dotenv
    import glob

    load_dotenv()

    raw_files = glob.glob("data/raw/*.json")

    if not raw_files:
        print("No JSON files found in data/raw/ — run fetch_data.py first")
    else:
        print(f"Found {len(raw_files)} file(s) to upload")
        results = upload_all(raw_files)

        client = get_s3_client()
        for s3_path in results:
            verify_upload(s3_path, client)