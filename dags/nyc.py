"""
## DAG Documentation
# This DAG is responsible for automating the monthly extraction of New York City taxi trip data and loading it into our Data Lake (MinIO).
"""
import sys
import os
from datetime import datetime
from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.nyc import download_yellow_tripdata
from utils.s3 import upload_file
from utils.nyc import fetch_yellow_tripdata

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dag(
    dag_id='nyc_taxi_ingestion',
    description='Pipeline Ingestion NYC Taxi (TaskFlow)',
    schedule='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=get_default_args()
)

def ingestion_pipeline():

    @task
    def download_taxi_nyc(logical_date):
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        
        logger.info(f"Dowloading data for {year}-{month}")

        return download_yellow_tripdata(year, month)
    
    @task
    def upload_taxi_nyc(file_path, logical_date):
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        
        logger.info(f"Uploading {file_path} to the Data Lake")
        
        file_name = os.path.basename(file_path)
        key_destino = f"raw/nyc_taxi/{year}/{month}/{file_name}"
        
        upload_file(filepath=file_path, bucket="nyc-taxi-lake", key=key_destino)

    downloaded_file = download_taxi_nyc()
    upload_taxi_nyc(downloaded_file)

pipeline = ingestion_pipeline()
