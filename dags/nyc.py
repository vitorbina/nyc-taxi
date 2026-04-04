"""
# NYC Taxi Ingestion

Downloads monthly trip records from the NYC TLC into the data lake.
Source: NYC TLC. Destination: MinIO bucket `data-lake-nyc`. Runs monthly.

TLC data has a 2-3 month lag. Skipped tasks mean the file isn't out yet, not a failure.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).
"""

from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.taxi import ingest_taxi_data
from utils.hive import repair_table, RAW_DATABASE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"

TAXI_MAPPING = {
    "yellow_taxi": "yellow",
    "green_taxi": "green",
    "app_rides": "fhv",
    "high_volume_fhv": "fhvhv",
}


@dag(
    **get_default_args(
        dag_id="taxi_ingestion",
        description="Monthly taxi trip data ingestion pipeline (NYC TLC)",
        schedule="@monthly",
        catchup=True,
        doc_md=__doc__,
    )
)
def ingestion_pipeline():

    @task
    def ingest_taxi_type(taxi_type: str, lake_folder: str, logical_date=None):
        ingest_taxi_data(
            taxi_type=taxi_type,
            lake_folder=lake_folder,
            logical_date=logical_date,
            bucket=BUCKET,
        )

    @task
    def update_hive(lake_folder: str):
        repair_table(lake_folder, database=RAW_DATABASE)

    for folder_name, source_name in TAXI_MAPPING.items():
        ingest_task = ingest_taxi_type.override(task_id=f"ingest_{folder_name}")(
            taxi_type=source_name,
            lake_folder=folder_name
        )
        hive_task = update_hive.override(task_id=f"hive_{folder_name}")(lake_folder=folder_name)
        ingest_task >> hive_task


pipeline = ingestion_pipeline()
