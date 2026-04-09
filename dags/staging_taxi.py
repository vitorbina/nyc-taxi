"""
# NYC Taxi Staging

Reads raw taxi parquet files from MinIO, applies cleaning, type casting,
code translation, and zone enrichment in a single step, then writes
the result to the staging layer.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).
Runs monthly, triggered after the raw ingestion DAG completes.
"""

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
import logging
from utils.default import get_default_args
from utils.s3 import file_exists
from utils.staging.yellow import stage_yellow
from utils.staging.green import stage_green
from utils.staging.fhv import stage_fhv
from utils.staging.hvfhv import stage_hvfhv
from utils.hive import repair_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"

STAGING_MAPPING = {
    "yellow_taxi": stage_yellow,
    "green_taxi": stage_green,
    "app_rides": stage_fhv,
    "high_volume_fhv": stage_hvfhv,
}


@dag(
    **get_default_args(
        dag_id="taxi_staging",
        description="Monthly staging pipeline for NYC taxi trip data",
        schedule="@monthly",
        catchup=False,
        doc_md=__doc__,
    )
)
def staging_pipeline():

    @task.sensor(task_id="wait_for_ingestion", mode="reschedule", timeout=3600, poke_interval=120)
    def wait_for_ingestion(logical_date=None) -> PokeReturnValue:
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        key = f"raw/yellow_taxi/partition_date={year}-{int(month):02d}-01/yellow_tripdata_{year}-{int(month):02d}.parquet"
        return PokeReturnValue(is_done=file_exists(bucket=BUCKET, key=key))

    @task
    def stage_taxi_type(lake_folder: str, logical_date=None):
        stage_fn = STAGING_MAPPING[lake_folder]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        stage_fn(lake_folder=lake_folder, year=year, month=month, bucket=BUCKET)

    @task
    def update_hive(lake_folder: str):
        repair_table(lake_folder)

    sensor = wait_for_ingestion()

    for folder_name in STAGING_MAPPING:
        stage_task = stage_taxi_type.override(task_id=f"stage_{folder_name}")(lake_folder=folder_name)
        hive_task = update_hive.override(task_id=f"hive_{folder_name}")(lake_folder=folder_name)
        sensor >> stage_task >> hive_task


pipeline = staging_pipeline()
