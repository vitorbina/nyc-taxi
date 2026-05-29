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

FINAL_TABLES = ["trips", "revenue", "weather_impact", "zones_geo"]


@dag(
    **get_dag_config(
        dag_id="taxi_final",
        description="Aggregation into the final layer, triggered by staging completion",
        schedule=AssetAll(*staging_taxi_assets.values()),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
        tags=["taxi", "final"],
    )
)
def final_pipeline():

    @task
    def build_trips():
        logger.info("Building final.trips")
        compute_trips(bucket=BUCKET)

    @task
    def build_revenue_by_zone():
        logger.info("Building final.revenue")
        compute_revenue_by_zone(bucket=BUCKET)

    @task
    def build_weather_impact():
        logger.info("Building final.weather_impact")
        compute_weather_impact(bucket=BUCKET)

    @task
    def build_zones_geo():
        logger.info("Building final.zones_geo")
        compute_zones_geo(bucket=BUCKET)

    @task
    def register_final_tables():
        logger.info("Registering final tables: %s", FINAL_TABLES)
        setup_hive(tables=FINAL_TABLES, database=FINAL_DATABASE, location_prefix="final", bucket=BUCKET)

    trips = build_trips()
    revenue = build_revenue_by_zone()
    weather = build_weather_impact()
    zones_geo = build_zones_geo()
    register = register_final_tables()

    # zones_geo depends on final.revenue existing; trips and weather are independent
    # kept sequential to avoid Spark worker resource contention on single-worker setup
    trips >> weather >> revenue >> zones_geo >> register


pipeline = final_pipeline()
