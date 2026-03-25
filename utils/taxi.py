import os
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def download_taxi_data(taxi_type: str, year: str, month: str, local_path: str) -> None:
    file_name = os.path.basename(local_path)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

    logger.info(f"Downloading from {url}")

    with requests.get(url, stream=True) as r:
        if r.status_code in [403, 404]:
            logger.warning(f"File not found on server (404/403). Skipping {taxi_type} for {year}-{month}.")
            raise AirflowSkipException(f"Data not available yet: {url}")
        r.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    logger.info(f"Download finished. File saved at: {local_path}")


def upload_taxi(lake_folder: str, local_path: str, year: str, month: str, bucket: str) -> None:
    file_name = os.path.basename(local_path)
    key = f"raw/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"

    logger.info(f"Uploading {local_path} to {bucket}/{key}")
    upload_file(filepath=local_path, bucket=bucket, key=key)


def ingest_taxi_data(lake_folder: str, taxi_type: str, logical_date, bucket: str) -> None:
    year = str(logical_date.year)
    month = str(logical_date.month)
    file_name = f"{taxi_type}_tripdata_{year}-{int(month):02d}.parquet"

    tmpfolder = tempfile.mkdtemp()
    try:
        local_path = os.path.join(tmpfolder, file_name)
        download_taxi_data(taxi_type, year, month, local_path)
        upload_taxi(lake_folder, local_path, year, month, bucket)
    finally:
        shutil.rmtree(tmpfolder)
