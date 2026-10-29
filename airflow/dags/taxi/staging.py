"""
# NYC Taxi Staging

Runs the dbt staging models (dbt/models/staging/taxi): cleaning, type casting,
code translation and zone enrichment, written to MinIO partitioned by partition_date.

Covers yellow taxi, green taxi, FHV (app rides) and High Volume FHV (Uber, Lyft, Via).
Triggered when all four raw taxi assets have been updated by the taxi_ingestion DAG.

Each table is rebuilt from the whole raw layer by DuckDB, then tested; only a table
whose tests pass is registered in the catalog and announced to the final layer.
"""

import os

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from utils.dbt_tasks import dbt_task_group
from utils.hive import repair_table
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
    )
)
def staging_pipeline():

    @task
    def update_hive(lake_folder: str):
        repair_table(lake_folder, bucket=BUCKET)

    for folder_name, asset in staging_taxi_assets.items():
        stage = dbt_task_group(f"stage_{folder_name}", select=[f"stg_{folder_name}"])
        hive_task = update_hive.override(task_id=f"hive_{folder_name}", outlets=[asset])(lake_folder=folder_name)
        stage >> hive_task


pipeline = staging_pipeline()
