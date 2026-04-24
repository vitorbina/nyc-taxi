import logging
import os
import shutil
import tempfile
from datetime import datetime

import requests

from airflow.exceptions import AirflowSkipException

from utils.s3 import upload_file

logger = logging.getLogger(__name__)

TLC_TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _download_taxi_file(local_path: str) -> None:
    file_name = os.path.basename(local_path)
    url = f"{TLC_TRIP_DATA_URL}/{file_name}"

    logger.info("Downloading from %s", url)

    with requests.get(url, stream=True) as r:
        if r.status_code in [403, 404]:
            logger.warning("File not available on TLC CDN (status %s). Skipping.", r.status_code)
            raise AirflowSkipException(f"Data not available yet: {url}")
        r.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    logger.info("Download finished: %s", local_path)


def _upload_taxi_file(local_path: str, lake_folder: str, year: str, month: str, bucket: str) -> None:
    file_name = os.path.basename(local_path)
    key = f"raw/{lake_folder}/partition_date={year}-{int(month):02d}-01/{file_name}"
    logger.info("Uploading %s to %s/%s", local_path, bucket, key)
    upload_file(filepath=local_path, bucket=bucket, key=key)


def ingest_taxi_data(lake_folder: str, taxi_type: str, logical_date: datetime, bucket: str) -> None:
    year = str(logical_date.year)
    month = str(logical_date.month)
    file_name = f"{taxi_type}_tripdata_{year}-{int(month):02d}.parquet"

    tmpdir = tempfile.mkdtemp()
    try:
        local_path = os.path.join(tmpdir, file_name)
        _download_taxi_file(local_path)
        _upload_taxi_file(local_path, lake_folder, year, month, bucket)
    finally:
        shutil.rmtree(tmpdir)
