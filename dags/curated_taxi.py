"""
# NYC Taxi Curated

Reads staged taxi parquet files from MinIO, translates numeric codes to
human-readable labels, enriches trips with pickup and dropoff zone names,
and writes the result to the curated layer.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).

Runs monthly, after the staging DAG completes.
"""

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
import logging
from utils.default import get_default_args
from utils.s3 import file_exists
from utils.curated.yellow import curate_yellow
from utils.curated.green import curate_green
from utils.curated.fhv import curate_fhv
from utils.curated.hvfhv import curate_hvfhv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"

TAXI_MAPPING = {
    "yellow_taxi": curate_yellow,
    "green_taxi": curate_green,
    "app_rides": curate_fhv,
    "high_volume_fhv": curate_hvfhv,
}


@dag(
    **get_default_args(
        dag_id="nyc_taxi_curated",
        description="Monthly curated pipeline for NYC taxi trip data",
        schedule="@monthly",
        catchup=True,
        dag_file=__file__,
        doc_md=__doc__,
    )
)
def curated_pipeline():

    @task.sensor(task_id="wait_for_staging", mode="reschedule", timeout=3600, poke_interval=120)
    def wait_for_staging(logical_date=None) -> PokeReturnValue:
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        key = f"staging/yellow_taxi/partition_date={year}-{int(month):02d}-01/yellow_tripdata_{year}-{int(month):02d}.parquet"
        return PokeReturnValue(is_done=file_exists(bucket=BUCKET, key=key))

    @task
    def curate_taxi_type(lake_folder: str, logical_date=None):
        curate_fn = TAXI_MAPPING[lake_folder]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        curate_fn(lake_folder=lake_folder, year=year, month=month, bucket=BUCKET)

    curated_tasks = [
        curate_taxi_type.override(task_id=f"curate_{folder_name}")(
            lake_folder=folder_name
        )
        for folder_name in TAXI_MAPPING
    ]

    wait_for_staging() >> curated_tasks


pipeline = curated_pipeline()
