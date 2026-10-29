"""
# NYC Weather Staging

Runs the dbt model stg_weather: reads all raw weather JSON files from MinIO,
applies type casting and weather code translation, and writes the staging layer
partitioned by partition_date.

Triggered by the raw_weather asset, produced by the weather_ingestion DAG.

Reads the whole raw layer and overwrites staging in one pass: weather volume is
small (hourly records), so a full rewrite takes seconds.
"""

import os

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from utils.dbt_tasks import dbt_task_group
from utils.hive import repair_table
from utils.assets import raw_weather, staging_weather

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

    @task(outlets=[staging_weather])
    def update_hive():
        repair_table("weather", bucket=BUCKET)

    dbt_task_group("stage_weather", select=["stg_weather"]) >> update_hive()


pipeline = weather_staging_pipeline()
