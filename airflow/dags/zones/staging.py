"""
# NYC Zones Staging

Reads the raw taxi zone lookup CSV from MinIO, applies cleaning and type casting,
and writes a parquet file to the staging layer.

Runs once, triggered after the raw zones ingestion DAG completes.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from staging.zones import stage_zones
from staging.zones_geo import stage_zones_geo
from utils.assets import raw_taxi_zones

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")


@dag(
    **get_dag_config(
        dag_id="zones_staging",
        description="Staging pipeline for NYC taxi zone reference data, triggered by raw ingestion",
        schedule=AssetAll(raw_taxi_zones),
        doc_md=__doc__,
        tags=["zones", "staging"],
    )
)
def zones_staging_pipeline():

    @task
    def stage_zone_lookup():
        stage_zones(bucket=BUCKET)

    @task
    def stage_zone_geometry():
        stage_zones_geo(bucket=BUCKET)

    stage_zone_lookup()
    stage_zone_geometry()


pipeline = zones_staging_pipeline()
