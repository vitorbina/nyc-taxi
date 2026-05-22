"""
# NYC Weather Ingestion

Fetches daily historical weather data for New York City from the Open-Meteo Archive API.
Source: Open-Meteo (free, no API key). Destination: MinIO bucket `data-lake-nyc`. Runs daily.

Partition: `raw/weather/partition_date=YYYY-MM-DD/`
"""

import os
import logging

from airflow.decorators import dag, task

from utils.default import get_dag_config
from utils.weather import ingest_weather_data, WEATHER_RAW_SCHEMA
from utils.hive import repair_table
from utils.constants import RAW_DATABASE
from utils.assets import raw_weather

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="weather_ingestion",
        description="Daily weather data ingestion pipeline (Open-Meteo Archive API)",
        schedule="@daily",
        catchup=False,
        doc_md=__doc__,
        tags=["weather", "ingestion"],
    )
)
def weather_ingestion_pipeline():

    @task
    def ingest_daily_weather(data_interval_end=None):
        logger.info("Ingesting weather data for %s", data_interval_end)
        ingest_weather_data(
            execution_date=data_interval_end,
            bucket=BUCKET,
        )

    @task(outlets=[raw_weather])
    def update_hive():
        repair_table("weather", database=RAW_DATABASE, file_format="json", schema_ddl=WEATHER_RAW_SCHEMA)

    ingest_daily_weather() >> update_hive()


pipeline = weather_ingestion_pipeline()
