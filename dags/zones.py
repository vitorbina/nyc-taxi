"""
# NYC Zone Reference Ingestion

Ingests static reference files from the NYC TLC used to decode location IDs in trip data.
Source: NYC TLC. Destination: MinIO bucket `data-lake-nyc`. Runs once.

Path: `raw/reference/{file_name}/{file_name}`
"""

from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.zones import ingest_zone_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"


@dag(
    **get_default_args(
        dag_id="zones_ingestion",
        description="One-time ingestion of NYC TLC reference files (zone lookup and shapefile)",
        schedule="@once",
        doc_md=__doc__,
    )
)
def zones_ingestion_pipeline():

    @task
    def ingest_reference_file(file_name: str):
        ingest_zone_data(file_name=file_name, bucket=BUCKET)

    reference_files = [
        "taxi_zone_lookup.csv",
        "taxi_zones.zip",
    ]

    for file_name in reference_files:
        task_id = f"ingest_{file_name.replace('.csv', '').replace('.zip', '')}"
        ingest_reference_file.override(task_id=task_id)(file_name=file_name)


pipeline = zones_ingestion_pipeline()
