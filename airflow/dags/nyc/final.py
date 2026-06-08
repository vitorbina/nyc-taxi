"""
# NYC Taxi Final Layer

Reads from the staging Hive tables (staging.*) and produces three unified
tables written to MinIO under final/ and registered in the final database.

| Table          | Description                                                |
|----------------|------------------------------------------------------------|
| trips          | Trip-level data unified across the 4 taxi types            |
| revenue        | Trip revenue with pickup zone and borough enrichment       |
| weather_impact | Trip-level fares joined with daily weather conditions      |
| zones_geo      | Zone polygons (WKT) with aggregated trip and revenue stats |

Triggered when all four staging taxi assets have been updated by the taxi_staging DAG.

Each table is published as a unit — built and registered in the Hive catalog in
the same task — so downstream consumers (e.g. zones_geo, which reads final.revenue)
always find their dependencies in the catalog.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from utils.hive import setup_hive
from utils.constants import FINAL_DATABASE
from utils.assets import staging_taxi_assets
from final.trips import compute_trips
from final.revenue_by_zone import compute_revenue_by_zone
from final.weather_impact import compute_weather_impact
from final.zones_geo import compute_zones_geo

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")

FINAL_BUILDERS = {
    "trips": compute_trips,
    "revenue": compute_revenue_by_zone,
    "weather_impact": compute_weather_impact,
    "zones_geo": compute_zones_geo,
}


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
    def build_and_register(table: str):
        logger.info("Building and registering final.%s", table)
        FINAL_BUILDERS[table](bucket=BUCKET)
        setup_hive(tables=[table], database=FINAL_DATABASE, location_prefix="final", bucket=BUCKET)

    trips = build_and_register.override(task_id="build_trips")("trips")
    weather = build_and_register.override(task_id="build_weather_impact")("weather_impact")
    revenue = build_and_register.override(task_id="build_revenue")("revenue")
    zones_geo = build_and_register.override(task_id="build_zones_geo")("zones_geo")

    # Sequential to avoid Spark contention on the single-worker setup.
    # revenue is built and registered before zones_geo, which reads final.revenue
    # from the catalog.
    trips >> weather >> revenue >> zones_geo


pipeline = final_pipeline()
