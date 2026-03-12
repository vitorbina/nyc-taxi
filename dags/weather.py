from datetime import datetime
from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.weather import ingest_weather_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dag_doc_md = """
# NYC Weather Ingestion Pipeline

## Overview
This DAG fetches daily weather data for New York City from the OpenWeatherMap API
and stores it in the data lake.

## Architecture
* **Source**: Open-Meteo Archive API (historical weather — free, no API key required).
* **Destination**: MinIO | Bucket: data-lake-nyc.
* **Frequency**: Daily.
* **Partition**: `raw/weather/partition_date=YYYY-MM-DD/`
"""

@dag(
    **get_default_args(
        dag_id='nyc_weather_ingestion',
        description='Daily weather data ingestion pipeline (OpenWeatherMap API)',
        schedule='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        doc_md=dag_doc_md,
    )
)
def weather_ingestion_pipeline():

    @task
    def process_daily_weather(data_interval_end=None):
        ingest_weather_data(
            execution_date=data_interval_end,
            bucket="data-lake-nyc"
        )

    process_daily_weather()

pipeline = weather_ingestion_pipeline()