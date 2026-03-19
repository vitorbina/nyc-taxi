"""
# NYC Taxi Curated

Reads staged yellow taxi parquet files from MinIO, translates numeric codes to
human-readable labels, enriches trips with pickup and dropoff zone names,
and writes the result to the curated layer.

Runs monthly, after the staging DAG completes.
"""

from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.curated.yellow import curate_yellow

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"

TAXI_MAPPING = {
    "yellow_taxi": curate_yellow,
}


@dag(
    **get_default_args(
        dag_id="nyc_taxi_curated",
        description="Monthly curated pipeline for NYC taxi trip data",
        schedule="@monthly",
        dag_file=__file__,
        doc_md=__doc__,
    )
)
def curated_pipeline():

    @task
    def curate_taxi_type(lake_folder: str, logical_date=None):
        curate_fn = TAXI_MAPPING[lake_folder]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        curate_fn(lake_folder=lake_folder, year=year, month=month, bucket=BUCKET)

    for folder_name in TAXI_MAPPING:
        curate_taxi_type.override(task_id=f"curate_{folder_name}")(
            lake_folder=folder_name
        )


pipeline = curated_pipeline()
