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

Incremental: `plan` picks the months that any staging table rebuilt since the final
tables last did. The monthly tables (MONTHLY_TABLES) rebuild only those months; the
small ones (zones_geo, weather_daily) are recomputed whole from them. Weather is read
when a month is built: trigger with `full_refresh` to rebuild every month (after
changing a model's logic, or if old weather was corrected).

Every model is tested right after it is built, and the marts are reconciled with
trips at the end. The tables are registered in the catalog (and so reach the
dashboards) only once everything passed.
"""

import os

from airflow.decorators import dag, task
from airflow.sdk import AssetAll, Param

from utils.default import get_dag_config
from utils.dbt_tasks import dbt_task_group
from utils.hive import setup_hive
from utils.incremental import stale_partitions
from utils.paths import final_key, staging_key
from utils.constants import FINAL_DATABASE, FINAL_TABLES
from utils.assets import staging_taxi_assets

BUCKET = os.getenv("MINIO_BUCKET")

# Final tables partitioned by month (partition_by in their dbt config)
MONTHLY_TABLES = ["trips", "revenue", "weather_impact", "trips_hourly"]


@dag(
    **get_dag_config(
        dag_id="taxi_final",
        description="Aggregation into the final layer, triggered by staging completion",
        schedule=AssetAll(*staging_taxi_assets.values()),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
        tags=["layer:final", "domain:taxi"],
        params={"full_refresh": Param(False, type="boolean", description="Rebuild every month, not only the stale ones")},
    )
)
def final_pipeline():

    @task.short_circuit
    def plan(params=None) -> list[str]:
        return stale_partitions(
            BUCKET,
            sources=[staging_key(table) for table in staging_taxi_assets],
            targets=[final_key(table) for table in MONTHLY_TABLES],
            full_refresh=params["full_refresh"],
        )

    @task
    def register(table: str):
        setup_hive(tables=[table], database=FINAL_DATABASE, location_prefix="final", bucket=BUCKET)

    build = dbt_task_group("build_final", select=["path:models/final"], partitions_from="plan")
    plan() >> build
    for table in FINAL_TABLES:
        build >> register.override(task_id=f"hive_{table}")(table=table)


pipeline = final_pipeline()
