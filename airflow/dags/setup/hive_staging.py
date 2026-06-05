"""
# Hive Metastore Setup — Staging

Registers the staging tables in the Hive Metastore, pointing to their locations
in MinIO. Run after the staging layer has data.

After this DAG runs, staging data is queryable by table name:
`SELECT * FROM staging.yellow_taxi`
"""

import os

from airflow.decorators import dag, task

from utils.default import get_dag_config
from utils.hive import setup_hive
from utils.constants import STAGING_DATABASE

BUCKET = os.getenv("MINIO_BUCKET")

STAGING_TABLES = [
    "yellow_taxi",
    "green_taxi",
    "app_rides",
    "high_volume_fhv",
    "weather",
]


@dag(
    **get_dag_config(
        dag_id="hive_setup_staging",
        description="Register staging tables in the Hive Metastore",
        schedule="@once",
        doc_md=__doc__,
        tags=["domain:ops"],
    )
)
def hive_setup_staging_pipeline():

    @task
    def register_table(table: str, location_prefix: str = "staging"):
        setup_hive(
            tables=[table],
            database=STAGING_DATABASE,
            location_prefix=location_prefix,
            bucket=BUCKET,
        )

    for table in STAGING_TABLES:
        register_table.override(task_id=f"staging_{table}")(table=table)

    register_table.override(task_id="staging_taxi_zones_geo")(
        table="taxi_zones_geo", location_prefix="staging/reference"
    )


pipeline = hive_setup_staging_pipeline()
