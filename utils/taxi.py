import os
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def download_taxi_data(taxi_type, year, month):

    tmpfolder = tempfile.mkdtemp()

    file_name = f"{taxi_type}_tripdata_{year}-{int(month):02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = os.path.join(tmpfolder, file_name)

    logger.info(f"Downloading from {url}")

    with requests.get(url, stream=True) as r:
        if r.status_code in [403, 404]:
            logger.warning(f"File not found on server (404/403). Skipping {taxi_type} for {year}-{month}.")
            shutil.rmtree(tmpfolder)
            raise AirflowSkipException(f"Data not available yet: {url}")
        r.raise_for_status()
    
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    logger.info(f"Download finished. File saved at: {local_path}")

    return local_path

def upload_taxi(lake_folder, local_path, year, month, bucket):
    file_name = os.path.basename(local_path)

    key = f"raw/{lake_folder}/partition_date={year}-{int(month):02d}/{file_name}"

    logger.info(f"Uploading {local_path} to {bucket}/{key}")

    upload_file(filepath=local_path, bucket=bucket, key=key)
    
    tmpfolder = os.path.dirname(local_path)
    shutil.rmtree(tmpfolder)
    logger.info(f"Automatic cleanup: temporary folder {tmpfolder} successfully deleted!")


def ingest_taxi_data(lake_folder, taxi_type, logical_date, bucket):
    year = str(logical_date.year)
    month = str(logical_date.month)

    local_path = download_taxi_data(taxi_type, year, month)
    upload_taxi(lake_folder, local_path, year, month, bucket)