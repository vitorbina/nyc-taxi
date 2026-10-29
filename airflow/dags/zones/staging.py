"""
# NYC Zones Staging

Runs the dbt model stg_taxi_zone_lookup (raw zone lookup CSV, cleaned and typed)
and parses the zone shapefile into polygons with geopandas, which SQL can't do.

Runs once, triggered after the raw zones ingestion DAG completes.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from utils.dbt_tasks import dbt_task_group
from staging.zones_geo import stage_zones_geo
from utils.hive import setup_hive
from utils.constants import STAGING_DATABASE
from utils.assets import raw_taxi_zones

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="zones_staging",
        description="Staging pipeline for NYC taxi zone reference data, triggered by raw ingestion",
        schedule=AssetAll(raw_taxi_zones),
        doc_md=__doc__,
        max_active_runs=1,
        tags=["layer:staging", "domain:zones"],
    )
)
def zones_staging_pipeline():

    @task
    def stage_zone_geometry():
        stage_zones_geo(bucket=BUCKET)
        setup_hive(
            tables=["taxi_zones_geo"],
            database=STAGING_DATABASE,
            location_prefix="staging/reference",
            bucket=BUCKET,
        )

    dbt_task_group("stage_zone_lookup", select=["stg_taxi_zone_lookup"])
    stage_zone_geometry()


pipeline = zones_staging_pipeline()
