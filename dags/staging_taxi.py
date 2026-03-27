"""
# NYC Taxi Staging

Reads raw taxi parquet files from MinIO, applies cleaning and type casting,
and writes the result to the staging layer.

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"

TAXI_MAPPING = {
    "yellow_taxi": stage_yellow,
    "green_taxi": stage_green,
    "app_rides": stage_fhv,
    "high_volume_fhv": stage_hvfhv,
}


@dag(
    **get_default_args(
        dag_id="nyc_taxi_staging",
        description="Monthly staging pipeline for NYC taxi trip data",
        schedule="@monthly",
        catchup=True,
        dag_file=__file__,
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
        stage_fn = TAXI_MAPPING[lake_folder]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        stage_fn(lake_folder=lake_folder, year=year, month=month, bucket=BUCKET)

    staging_tasks = [
        stage_taxi_type.override(task_id=f"stage_{folder_name}")(
            lake_folder=folder_name
        )
        for folder_name in TAXI_MAPPING
    ]

    wait_for_ingestion() >> staging_tasks


pipeline = staging_pipeline()
