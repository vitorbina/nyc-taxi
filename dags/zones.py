from datetime import datetime
from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.zones import ingest_zone_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dag_doc_md = """
# NYC Zone Reference Ingestion

This DAG ingests static reference files from the NYC TLC that are used to decode
location and base IDs found in the trip data. It runs once and only needs to be
re-triggered if the source files are updated by the TLC.

**Source**: NYC TLC (https://d37ci6vzurychx.cloudfront.net/misc/)
**Destination**: MinIO | Bucket: data-lake-nyc
**Path**: `raw/reference/{file_name}/{file_name}.csv`
"""


@dag(
    **get_default_args(
        dag_id='nyc_zones_ingestion',
        description='One-time ingestion of NYC TLC reference files (zone lookup and FHV bases)',
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
        "fhv_bases.csv",
        "taxi_zones.zip",
    ]

    for file_name in reference_files:
        task_id = f"ingest_{file_name.replace('.csv', '').replace('.zip', '')}"
        ingest_reference_file.override(task_id=task_id)(file_name=file_name)


pipeline = zones_ingestion_pipeline()
