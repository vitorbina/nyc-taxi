"""
# Hive Metastore Setup — Raw

Registers the raw tables in the Hive Metastore, pointing to their locations in
MinIO. Run after the raw layer has data (e.g. after a backfill that used
skip_repair).

After this DAG runs, raw data is queryable by table name: `SELECT * FROM raw.yellow_taxi`
"""

import os

from airflow.decorators import dag, task

from utils.default import get_dag_config
from utils.hive import setup_hive
from utils.constants import RAW_DATABASE
from utils.weather import WEATHER_RAW_SCHEMA

BUCKET = os.getenv("MINIO_BUCKET")

RAW_TABLES = [
    "yellow_taxi",
    "green_taxi",
    "app_rides",
    "high_volume_fhv",
    "weather",
]

RAW_TABLE_CONFIG = {
    "weather": {"file_format": "json", "schema_ddl": WEATHER_RAW_SCHEMA},
}


@dag(
    **get_dag_config(
        dag_id="hive_setup_raw",
        description="Register raw tables in the Hive Metastore",
        schedule="@once",
        doc_md=__doc__,
        tags=["domain:ops"],
    )
)
def hive_setup_raw_pipeline():

    @task
    def register_table(table: str, file_format: str = "parquet", schema_ddl: str = None):
        setup_hive(
            tables=[table],
            database=RAW_DATABASE,
            location_prefix="raw",
            bucket=BUCKET,
            file_format=file_format,
            schema_ddl=schema_ddl,
        )

    for table in RAW_TABLES:
        config = RAW_TABLE_CONFIG.get(table, {})
        register_table.override(task_id=f"raw_{table}")(table=table, **config)


pipeline = hive_setup_raw_pipeline()
