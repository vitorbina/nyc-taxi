import os
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

 
# Creates a temporary folder, downloads the file, and returns the local path.
def download_yellow_tripdata(year, month):

    tmpfolder = tempfile.mkdtemp()

    file_name = f"yellow_tripdata_{year}-{int(month):02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = os.path.join(tmpfolder, file_name)

    logger.info(f"Downloading from {url}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                
    logger.info(f"Download finished. File saved at: {local_path}")

    return local_path

# It orchestrates downloading and uploading using a tempdir.
def upload_yellow_tripdata(local_path, year, month, bucket):
    file_name = os.path.basename(local_path)

    key = f"raw/nyc_taxi/year_month={year}-{int(month):02d}/{file_name}"

    logger.info(f"Uploading {local_path} to {bucket}/{key}")

    upload_file(filepath=local_path, bucket=bucket, key=key)
    
    tmpfolder = os.path.dirname(local_path)
    shutil.rmtree(tmpfolder)
    logger.info(f"Automatic cleanup: temporary folder {tmpfolder} successfully deleted!")