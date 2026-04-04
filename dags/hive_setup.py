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
from utils.hive import setup_hive, RAW_DATABASE

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
    def register_staging_tables():
        setup_hive(tables=STAGING_TABLES)

    @task
    def register_raw_tables():
        setup_hive(tables=RAW_TABLES, database=RAW_DATABASE, location_prefix="raw")

    register_staging_tables()
    register_raw_tables()


pipeline = hive_setup_pipeline()
