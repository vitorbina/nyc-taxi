import logging
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_ingestion(year, month, bucket_name):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    
    object_path = f"raw/{year}/{month}/{file_name}"

    logging.info(f"Starting ingestion process for: {file_name}")

    hook = S3Hook(aws_conn_id='minio_conn')
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()

            hook.load_file_obj(
                file_obj=r.raw,
                key=object_path,
                bucket_name=bucket_name,
                replace=True
            )
            
        logging.info(f"Successfully uploaded to {bucket_name}/{object_path}")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
