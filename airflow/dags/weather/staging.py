"""
# NYC Weather Staging

Reads all raw weather JSON files from MinIO in a single Spark job, applies type
casting and weather code translation, and writes the staging layer partitioned
by partition_date.

Triggered by the raw_weather asset, produced by the weather_ingestion DAG.

Reads the whole raw layer and overwrites staging in one pass — weather volume is
small (hourly records), so a full rewrite is cheaper than per-day Spark sessions.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import AssetAll
from airflow.utils.trigger_rule import TriggerRule

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
        schedule=AssetAll(raw_weather),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
        tags=["layer:staging", "domain:weather"],
    )
)
def weather_staging_pipeline():

    @task
    def stage_all_weather():
        stage_weather(bucket=BUCKET)

    @task(outlets=[staging_weather], trigger_rule=TriggerRule.ALL_DONE)
    def update_hive():
        repair_table("weather")

    stage_all_weather() >> update_hive()


pipeline = weather_staging_pipeline()
