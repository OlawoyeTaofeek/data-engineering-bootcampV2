import os
import sys
import time
import logging
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import requests
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# ===============================
# Configuration
# ===============================

BASE_URL: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS: List[str] = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR: str = "."
CHUNK_SIZE: int = 8 * 1024 * 1024
MAX_DOWNLOAD_WORKERS: int = 4
MAX_UPLOAD_RETRIES: int = 3
VERIFY_RETRIES: int = 3

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ===============================
# Environment Validation
# ===============================

CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
print(CREDENTIALS_FILE)
BUCKET_NAME = os.getenv("BUCKET_NAME")
print(BUCKET_NAME)

if not CREDENTIALS_FILE or not BUCKET_NAME:
    logging.error("Missing required environment variables.")
    sys.exit(1)

# ===============================
# GCS Client
# ===============================

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

# ===============================
# Utility Functions
# ===============================

def ensure_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ===============================
# Download
# ===============================

def download_file(month: str) -> Optional[str]:
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        logging.info(f"Downloading {url}")
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(10000):
                    f.write(chunk)

        logging.info(f"Downloaded {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed for {month}: {e}")
        return None

## A function to create bucket now

def create_bucket(bucket_name: str):
    """

    :type bucket_name: str
    """
    try:
        # Get bucket details
        client.get_bucket(bucket_name)
        # check if bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            logging.info(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            logging.info(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)
    except NotFound:
        client.create_bucket(bucket_name)
        logging.info(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists, but you don't have access to see details
        logging.error(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)

def verify_gcs_upload(blob_name: str) -> bool:
    blob = bucket.blob(blob_name)
    return blob.exists()

def upload_to_gcs(file_path: str):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(MAX_UPLOAD_RETRIES):
        try:
            logging.info(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                logging.info(f"Verification successful for {blob_name}")
                return
            else:
                logging.warning(f"Verification failed for {blob_name}, retrying...")

        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        time.sleep(5)
    logging.error(f"Giving up on {file_path} after {MAX_UPLOAD_RETRIES} attempts.")

def cleanup_local_file(file_path: str):
    try:
        os.remove(file_path)
        logging.info(f"Deleted local file {file_path}")
    except Exception as e:
        logging.warning(f"Failed to delete {file_path}: {e}")


def run_pipeline():
    ensure_download_dir()
    create_bucket(BUCKET_NAME)

    logging.info("Starting the concurrent download....")
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove non values

    logging.info("All files processed and verified")
    for file_path in file_paths:
        cleanup_local_file(file_path)

if __name__ == "__main__":
    run_pipeline()



#def run_pipeline():
    # ensure_download_dir()
    # create_bucket()
    #
    # logging.info("Starting concurrent downloads...")
    #
    # with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
    #     futures = {executor.submit(download_file, month): month for month in MONTHS}
    #
    #     for future in as_completed(futures):
    #         month = futures[future]
    #         file_path = future.result()
    #
    #         if not file_path:
    #             logging.warning(f"Skipping upload for {month}")
    #             continue
    #
    #         success = upload_to_gcs(file_path)
    #
    #         if success:
    #             cleanup_local_file(file_path)
    #         else:
    #             logging.error(f"Upload failed permanently for {file_path}")
