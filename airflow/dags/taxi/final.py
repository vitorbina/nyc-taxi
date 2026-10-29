"""
# NYC Taxi Final Layer

Runs the dbt final models (dbt/models/final), which read the staging tables with
the trip quality rules applied, and writes them to MinIO under final/.

| Table          | Description                                                |
|----------------|------------------------------------------------------------|
| trips          | Trip-level data unified across the 4 taxi types            |
| revenue        | Trip revenue with pickup zone and borough enrichment       |
| weather_impact | Trip-level fares joined with daily weather conditions      |
| zones_geo      | Zone polygons with aggregated trip and revenue stats       |
| trips_hourly   | Dashboard mart: trips, revenue, distance, duration summed per hour x service x zone |
| weather_daily  | Dashboard mart: one row per day with its weather and total trips |

Triggered when all four staging taxi assets have been updated by the taxi_staging DAG.

Every model is tested right after it is built, and the marts are reconciled with
trips at the end. The tables are registered in the catalog (and so reach the
dashboards) only once everything passed.
"""

import os

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from utils.dbt_tasks import dbt_task_group
from utils.hive import setup_hive
from utils.constants import FINAL_DATABASE, FINAL_TABLES
from utils.assets import staging_taxi_assets

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="taxi_final",
        description="Aggregation into the final layer, triggered by staging completion",
        schedule=AssetAll(*staging_taxi_assets.values()),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
        tags=["layer:final", "domain:taxi"],
    )
)
def final_pipeline():

    @task
    def register(table: str):
        setup_hive(tables=[table], database=FINAL_DATABASE, location_prefix="final", bucket=BUCKET)

    build = dbt_task_group("build_final", select=["path:models/final"])
    for table in FINAL_TABLES:
        build >> register.override(task_id=f"hive_{table}")(table=table)


pipeline = final_pipeline()
