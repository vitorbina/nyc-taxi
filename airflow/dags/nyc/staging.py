"""
# NYC Taxi Staging

Reads raw taxi parquet files from MinIO, applies cleaning, type casting,
code translation, and zone enrichment in a single step, then writes
the result to the staging layer.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).
Triggered when all four raw taxi assets have been updated by the taxi_ingestion DAG.

Auto-discovers missing partitions: each task lists what's in raw and what's
already in staging, then processes the difference. Idempotent and resilient
to backfills.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from staging.yellow import stage_yellow
from staging.green import stage_green
from staging.fhv import stage_fhv
from staging.hvfhv import stage_hvfhv
from utils.hive import repair_table
from utils.assets import raw_taxi_assets, staging_taxi_assets
from utils.s3 import list_partitions
from utils.paths import raw_key, staging_key

logger = logging.getLogger(__name__)

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
        tags=["taxi", "staging"],
    )
)
def staging_pipeline():

    @task
    def stage_taxi_type(lake_folder: str):
        raw_partitions = set(list_partitions(BUCKET, raw_key(lake_folder)))
        staging_partitions = set(list_partitions(BUCKET, staging_key(lake_folder)))
        missing = sorted(raw_partitions - staging_partitions)

        if not missing:
            raise AirflowSkipException(f"No new partitions to stage for {lake_folder}")

        stage_fn = STAGING_MAPPING[lake_folder]
        logger.info("Staging %d new partition(s) for %s: %s", len(missing), lake_folder, missing)
        for partition in missing:
            year, month, _ = partition.split("-")
            stage_fn(lake_folder=lake_folder, year=year, month=month, bucket=BUCKET)

    @task
    def update_hive(lake_folder: str):
        repair_table(lake_folder)

    for folder_name in STAGING_MAPPING:
        stage_task = stage_taxi_type.override(task_id=f"stage_{folder_name}")(lake_folder=folder_name)
        hive_task = update_hive.override(
            task_id=f"hive_{folder_name}",
            outlets=[staging_taxi_assets[folder_name]],
        )(lake_folder=folder_name)
        stage_task >> hive_task


pipeline = staging_pipeline()
