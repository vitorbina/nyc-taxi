"""
# NYC Weather Staging

Reads raw weather JSON files from MinIO, converts hourly arrays into
row-per-hour parquet files with type casting and weather code descriptions,
then writes to the staging layer.

Triggered by the raw_weather asset, produced by the weather_ingestion DAG.
"""

import os
import logging

from airflow.decorators import dag, task

from utils.default import get_dag_config
from staging.weather import stage_weather
from utils.hive import repair_table
from utils.assets import raw_weather, staging_weather

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="weather_staging",
        description="Staging pipeline for NYC weather data, triggered by raw ingestion",
        schedule=[raw_weather],
        catchup=False,
        doc_md=__doc__,
    )
)
def weather_staging_pipeline():

    @task
    def stage_daily_weather(data_interval_end=None):
        date_str = data_interval_end.strftime("%Y-%m-%d")
        stage_weather(date_str=date_str, bucket=BUCKET)

    @task(outlets=[staging_weather])
    def update_hive():
        repair_table("weather")

    stage_daily_weather() >> update_hive()


pipeline = weather_staging_pipeline()
