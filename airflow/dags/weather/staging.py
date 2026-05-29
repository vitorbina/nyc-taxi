"""
# NYC Weather Staging

Reads raw weather parquet files from MinIO, applies type casting and
weather code translation, then writes to the staging layer.

Triggered by the raw_weather asset, produced by the weather_ingestion DAG.

Auto-discovers missing partitions: lists what's in raw and what's already
in staging, then processes the difference. Idempotent and resilient to
backfills.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from staging.weather import stage_weather
from utils.hive import repair_table
from utils.assets import raw_weather, staging_weather
from utils.s3 import list_partitions
from utils.paths import raw_key, staging_key

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="weather_staging",
        description="Staging pipeline for NYC weather data, triggered by raw ingestion",
        schedule=AssetAll(raw_weather),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
        tags=["weather", "staging"],
    )
)
def weather_staging_pipeline():

    @task
    def stage_daily_weather():
        raw_partitions = set(list_partitions(BUCKET, raw_key("weather")))
        staging_partitions = set(list_partitions(BUCKET, staging_key("weather")))
        missing = sorted(raw_partitions - staging_partitions)

        if not missing:
            raise AirflowSkipException("No new weather partitions to stage")

        logger.info("Staging %d new weather partition(s): %s", len(missing), missing)
        for date_str in missing:
            stage_weather(date_str=date_str, bucket=BUCKET)

    @task(outlets=[staging_weather])
    def update_hive():
        repair_table("weather")

    stage_daily_weather() >> update_hive()


pipeline = weather_staging_pipeline()
