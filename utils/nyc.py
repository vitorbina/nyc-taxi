import os
import logging
import requests
import tempfile
from utils.s3 import upload_file

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

 
# Download the file and save it in the received tmpfolder.
def download_yellow_tripdata(year, month):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = f"/tmp/{file_name}"
    logger.info(f"Downloading from {url}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                
    logger.info(f"Download finished. File saved at: {local_path}")
    return local_path


# It orchestrates downloading and uploading using a tempdir.
def fetch_yellow_tripdata(year, month, bucket):

    with tempfile.TemporaryDirectory() as tmpfolder:
        logger.info(f"Pasta temporária criada pelo SO: {tmpfolder}") 
        
        file_path = download_yellow_tripdata(year, month, tmpfolder)

        file_name = os.path.basename(file_path)
        
        key = f"raw/nyc_taxi/{year}/{month}/{file_name}"

        upload_file(filepath=file_path, bucket=bucket, key=key)
        
    logger.info("Automatic cleanup: temporary folder successfully deleted!")