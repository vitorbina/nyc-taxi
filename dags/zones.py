"""
# NYC Zone Reference Ingestion

Ingests static reference files from the NYC TLC used to decode location IDs in trip data.
Source: NYC TLC. Destination: MinIO bucket `data-lake-nyc`. Runs once.

Path: `raw/reference/{file_name}/{file_name}`
"""

import os
import logging

from airflow.decorators import dag, task

from utils.default import get_dag_config
from utils.zones import ingest_zone_data
from utils.assets import raw_taxi_zones

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")

REFERENCE_FILES = [
    "taxi_zone_lookup.csv",
    "taxi_zones.zip",
]


@dag(
    **get_dag_config(
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

    for file_name in REFERENCE_FILES:
        stem = os.path.splitext(file_name)[0]
        outlets = [raw_taxi_zones] if stem == "taxi_zone_lookup" else []
        ingest_reference_file.override(task_id=f"ingest_{stem}", outlets=outlets)(file_name=file_name)


pipeline = zones_ingestion_pipeline()
