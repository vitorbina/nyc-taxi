"""
# Hive Metastore Setup

Registers all staging tables in the Hive Metastore, pointing to their
respective locations in MinIO. Runs once after the first ingestion cycle.

After this DAG runs, Spark jobs can query data by table name instead of
S3A paths: `SELECT * FROM nyc_taxi.yellow_taxi`
"""

from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.hive import setup_hive, DATABASE, RAW_DATABASE, FINAL_DATABASE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

STAGING_TABLES = [
    "yellow_taxi",
    "green_taxi",
    "app_rides",
    "high_volume_fhv",
    "weather",
]

RAW_TABLES = [
    "yellow_taxi",
    "green_taxi",
    "app_rides",
    "high_volume_fhv",
]

FINAL_TABLES = [
    "trips_by_month",
    "revenue_by_zone",
    "weather_impact",
]


@dag(
    **get_default_args(
        dag_id="hive_setup",
        description="One-time registration of staging and raw tables in the Hive Metastore",
        schedule="@once",
        doc_md=__doc__,
    )
)
def hive_setup_pipeline():

    @task
    def register_table(table: str, database: str, location_prefix: str):
        setup_hive(tables=[table], database=database, location_prefix=location_prefix)

    for table in STAGING_TABLES:
        register_table.override(task_id=f"staging_{table}")(
            table=table, database=DATABASE, location_prefix="staging"
        )

    for table in RAW_TABLES:
        register_table.override(task_id=f"raw_{table}")(
            table=table, database=RAW_DATABASE, location_prefix="raw"
        )

    for table in FINAL_TABLES:
        register_table.override(task_id=f"final_{table}")(
            table=table, database=FINAL_DATABASE, location_prefix="final"
        )


pipeline = hive_setup_pipeline()
