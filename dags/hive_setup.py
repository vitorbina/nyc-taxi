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
from utils.hive import setup_hive, DATABASE, RAW_DATABASE

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
    def register_table(table: str, database: str, location_prefix: str):
        setup_hive(tables=[table], database=database, location_prefix=location_prefix)

    @task
    def show_tables():
        from utils.spark import get_spark
        spark = get_spark("hive_show_tables")
        for db in [DATABASE, RAW_DATABASE]:
            tables = spark.sql(f"SHOW TABLES IN {db}").collect()
            logger.info(f"Tables in {db}: {[t.tableName for t in tables]}")
        spark.stop()

    all_tasks = []

    for table in STAGING_TABLES:
        all_tasks.append(
            register_table.override(task_id=f"staging_{table}")(
                table=table, database=DATABASE, location_prefix="staging"
            )
        )

    for table in RAW_TABLES:
        all_tasks.append(
            register_table.override(task_id=f"raw_{table}")(
                table=table, database=RAW_DATABASE, location_prefix="raw"
            )
        )

    all_tasks >> show_tables()


pipeline = hive_setup_pipeline()
