from datetime import datetime
from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.zones import ingest_zone_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dag_doc_md = """
# NYC Zone Reference Ingestion

Ingests static reference files from the NYC TLC used to decode location IDs in trip data.
Source: NYC TLC. Destination: MinIO bucket `data-lake-nyc`. Runs once.

Path: `raw/reference/{file_name}/{file_name}`
"""

@dag(
    **get_default_args(
        dag_id='nyc_zones_ingestion',
        description='One-time ingestion of NYC TLC reference files (zone lookup and shapefile)',
        schedule='@once',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        doc_md=dag_doc_md,
    )
)
def zones_ingestion_pipeline():

    @task
    def ingest_reference_file(file_name: str):
        bucket_name = "data-lake-nyc"
        ingest_zone_data(file_name=file_name, bucket=bucket_name)

    reference_files = [
        "taxi_zone_lookup.csv",
        "taxi_zones.zip",
    ]

    for file_name in reference_files:
        task_id = f"ingest_{file_name.replace('.csv', '').replace('.zip', '')}"
        ingest_reference_file.override(task_id=task_id)(file_name=file_name)

pipeline = zones_ingestion_pipeline()
