"""
# Hive Metastore Setup — Final

Registers the final tables in the Hive Metastore, pointing to their locations in
MinIO. Run after the final layer has data.

After this DAG runs, final data is queryable by table name:
`SELECT * FROM final.trips`
"""

import os

from airflow.decorators import dag, task

from utils.default import get_dag_config
from utils.hive import setup_hive
from utils.constants import FINAL_DATABASE

BUCKET = os.getenv("MINIO_BUCKET")

FINAL_TABLES = [
    "trips",
    "revenue",
    "weather_impact",
    "zones_geo",
]


@dag(
    **get_dag_config(
        dag_id="hive_setup_final",
        description="Register final tables in the Hive Metastore",
        schedule="@once",
        doc_md=__doc__,
        tags=["domain:ops"],
    )
)
def hive_setup_final_pipeline():

    @task
    def register_table(table: str):
        setup_hive(
            tables=[table],
            database=FINAL_DATABASE,
            location_prefix="final",
            bucket=BUCKET,
        )

    for table in FINAL_TABLES:
        register_table.override(task_id=f"final_{table}")(table=table)


pipeline = hive_setup_final_pipeline()
