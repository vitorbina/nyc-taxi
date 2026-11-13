"""
# NYC Weather Ingestion

Fetches daily historical weather data for New York City from the Open-Meteo Archive API.
Source: Open-Meteo (free, no API key). Destination: MinIO bucket `data-lake-nyc`. Runs daily.

Each run fetches the day 5 days before its date (ARCHIVE_DELAY_DAYS): the archive only
has measured data by then; before that it returns estimates that would never be fixed.

Partition: `raw/weather/partition_date=YYYY-MM-DD/`
"""

import os
import logging

from airflow.decorators import dag, task

from utils.default import get_dag_config
from utils.weather import ingest_weather_data, weather_day, WEATHER_RAW_SCHEMA
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
        max_active_runs=2,
        tags=["layer:raw", "domain:weather"],
    )
)
def weather_ingestion_pipeline():

    @task
    def ingest_daily_weather(data_interval_end=None):
        day = weather_day(data_interval_end)
        logger.info("Run of %s: ingesting weather for %s", data_interval_end, day.date())
        ingest_weather_data(day=day, bucket=BUCKET)

    @task(outlets=[raw_weather])
    def update_hive(dag_run=None):
        if dag_run and dag_run.conf.get("skip_repair"):
            logger.info("skip_repair=true — skipping repair_table for weather")
            return
        repair_table("weather", database=RAW_DATABASE, file_format="json", schema_ddl=WEATHER_RAW_SCHEMA)

    ingest_daily_weather() >> update_hive()


pipeline = weather_ingestion_pipeline()
