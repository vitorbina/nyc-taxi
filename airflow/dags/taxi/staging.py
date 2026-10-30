"""
# NYC Taxi Staging

Runs the dbt staging models (dbt/models/staging/taxi): cleaning, type casting,
code translation and zone enrichment, written to MinIO partitioned by partition_date.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).
Triggered when all four raw taxi assets have been updated by the taxi_ingestion DAG.

Incremental: for each table, `plan_*` compares raw with staging and picks the months
that are new or were re-downloaded since staging last built them; DuckDB rebuilds only
those, one at a time. A table with nothing to do skips the rest of its branch. Trigger
with `full_refresh` to rebuild every month (after changing a model's logic).

Each rebuilt table is tested; only a table whose tests pass is registered in the
catalog and announced to the final layer.
"""

import os

from airflow.decorators import dag, task
from airflow.sdk import AssetAll, Param

from utils.default import get_dag_config
from utils.dbt_tasks import dbt_task_group
from utils.hive import repair_table
from utils.incremental import stale_partitions
from utils.paths import raw_key, staging_key
from utils.assets import raw_taxi_assets, staging_taxi_assets

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="taxi_staging",
        description="Staging pipeline for NYC taxi trip data, triggered by raw ingestion",
        schedule=AssetAll(*raw_taxi_assets.values()),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
        tags=["layer:staging", "domain:taxi"],
        params={"full_refresh": Param(False, type="boolean", description="Rebuild every month, not only the stale ones")},
    )
)
def staging_pipeline():

    @task.short_circuit
    def plan(lake_folder: str, params=None) -> list[str]:
        return stale_partitions(
            BUCKET,
            sources=[raw_key(lake_folder)],
            targets=[staging_key(lake_folder)],
            full_refresh=params["full_refresh"],
        )

    @task
    def update_hive(lake_folder: str):
        repair_table(lake_folder, bucket=BUCKET)

    for folder_name, asset in staging_taxi_assets.items():
        months = plan.override(task_id=f"plan_{folder_name}")(lake_folder=folder_name)
        stage = dbt_task_group(f"stage_{folder_name}", select=[f"stg_{folder_name}"], partitions_from=f"plan_{folder_name}")
        hive_task = update_hive.override(task_id=f"hive_{folder_name}", outlets=[asset])(lake_folder=folder_name)
        months >> stage >> hive_task


pipeline = staging_pipeline()
