
from datetime import datetime
from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.taxi import ingest_taxi_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dag_doc_md = """
# NYC Taxi Data Ingestion Pipeline

## Project Overview
This DAG orchestrates the monthly extraction of public taxi and ride-hailing trip records from the NYC Taxi & Limousine Commission (TLC).

## Data Availability & Lag
* **Publication Lag**: Please note that NYC TLC data is typically released with a **2 to 3-month delay**. 
* **Skip Logic**: If a task is marked as **'Skipped'**, it is likely because the source file is not yet available on the government servers. This is an expected behavior and not a pipeline failure.

## Architecture
* **Source**: NYC TLC.
* **Destination**: MinIO | Bucket: data-lake-nyc.
* **Frequency**: Monthly.

## Data Sources
1. **Yellow Taxi (`yellow_taxi`)**: Manhattan-centric taxis.
2. **Green Taxi (`green_taxi`)**: Street-hail liveries (outer boroughs).
3. **App Rides (`app_rides`)**: For-Hire Vehicles.
"""

@dag(
    **get_default_args(
        dag_id='nyc_taxi_ingestion',
        description='Monthly taxi trip data ingestion pipeline (NYC TLC)',
        schedule='@monthly',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        doc_md=dag_doc_md,
    )
)
def ingestion_pipeline():

    @task
    def process_taxi_color(taxi_type: str, lake_folder: str, logical_date=None):
        bucket_name = "data-lake-nyc"
        
        ingest_taxi_data(
            taxi_type=taxi_type,
            lake_folder=lake_folder,
            logical_date=logical_date,
            bucket=bucket_name
        )

    taxi_mapping = {
        'yellow_taxi': 'yellow',
        'green_taxi': 'green',
        'app_rides': 'fhv'
    }
    
    for folder_name, source_name in taxi_mapping.items():
        process_taxi_color.override(task_id=f"ingest_{folder_name}")(
            taxi_type=source_name,
            lake_folder=folder_name
        )

pipeline = ingestion_pipeline()