"""
# NYC Taxi Staging

Reads raw taxi parquet files from MinIO, applies cleaning, type casting,
code translation, and zone enrichment in a single step, then writes
the result to the staging layer.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).
Triggered when all four raw taxi assets have been updated by the taxi_ingestion DAG.

Each task auto-discovers the partitions missing from staging and processes them
one at a time within a single Spark session, writing partitioned by partition_date.
"""

import os

from airflow.decorators import dag, task
from airflow.sdk import AssetAll
from airflow.utils.trigger_rule import TriggerRule

from utils.default import get_dag_config
from staging.yellow import stage_yellow
from staging.green import stage_green
from staging.fhv import stage_fhv
from staging.hvfhv import stage_hvfhv
from utils.hive import repair_table
from utils.assets import raw_taxi_assets, staging_taxi_assets

BUCKET = os.getenv("MINIO_BUCKET")

STAGING_MAPPING = {
    "yellow_taxi": stage_yellow,
    "green_taxi": stage_green,
    "app_rides": stage_fhv,
    "high_volume_fhv": stage_hvfhv,
}


@dag(
    **get_dag_config(
        dag_id="taxi_staging",
        description="Staging pipeline for NYC taxi trip data, triggered by raw ingestion",
        schedule=AssetAll(*raw_taxi_assets.values()),
        catchup=False,
        doc_md=__doc__,
        max_active_tasks=1,
        max_active_runs=1,
        tags=["layer:staging", "domain:taxi"],
    )
)
def staging_pipeline():

    @task
    def stage_taxi_type(lake_folder: str):
        STAGING_MAPPING[lake_folder](bucket=BUCKET)

    @task
    def update_hive(lake_folder: str):
        repair_table(lake_folder)

    for folder_name in STAGING_MAPPING:
        stage_task = stage_taxi_type.override(task_id=f"stage_{folder_name}")(lake_folder=folder_name)
        hive_task = update_hive.override(
            task_id=f"hive_{folder_name}",
            outlets=[staging_taxi_assets[folder_name]],
            trigger_rule=TriggerRule.ALL_DONE,
        )(lake_folder=folder_name)
        stage_task >> hive_task


pipeline = staging_pipeline()
