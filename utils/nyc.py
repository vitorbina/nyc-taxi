import os
import logging
import requests
from utils.s3 import upload_file

def download_yellow_tripdata(year, month):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = f"/tmp/{file_name}"
    logging.info(f"Downloading from {url}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                
    logging.info(f"Download finished. File saved at: {local_path}")
    return local_path

def fetch_yellow_tripdata(year, month, bucket):
   
    file_path = download_yellow_tripdata(year, month)

    file_name = os.path.basename(file_path)
    key = f"raw/nyc_taxi/{year}/{month}/{file_name}"
    
    try:
        upload_file(filepath=file_path, bucket=bucket, key=key)

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Cleanup: Deleted temporary file {file_path}")