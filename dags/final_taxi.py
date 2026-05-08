"""
# NYC Taxi Final Layer

Reads from the staging Hive tables (staging.*) and produces three unified
tables written to MinIO under final/ and registered in the final database.

| Table          | Description                                                |
|----------------|------------------------------------------------------------|
| trips          | Trip-level data unified across the 4 taxi types            |
| revenue        | Trip revenue with pickup zone and borough enrichment       |
| weather_impact | Trip-level fares joined with daily weather conditions      |

Triggered when all four staging taxi assets have been updated by the taxi_staging DAG.
"""

import os
import logging

from airflow.decorators import dag, task
from airflow.sdk import AssetAll

from utils.default import get_dag_config
from utils.hive import setup_hive, FINAL_DATABASE
from utils.assets import staging_taxi_assets
from final.trips import compute_trips
from final.revenue_by_zone import compute_revenue_by_zone
from final.weather_impact import compute_weather_impact

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET")

FINAL_TABLES = ["trips", "revenue", "weather_impact"]


@dag(
    **get_dag_config(
        dag_id="taxi_final",
        description="Aggregation into the final layer, triggered by staging completion",
        schedule=AssetAll(*staging_taxi_assets.values()),
        catchup=False,
        doc_md=__doc__,
        max_active_runs=1,
    )
)
def final_pipeline():

    @task
    def build_trips():
        compute_trips(bucket=BUCKET)

    @task
    def build_revenue_by_zone():
        compute_revenue_by_zone(bucket=BUCKET)

    @task
    def build_weather_impact():
        compute_weather_impact(bucket=BUCKET)

    @task
    def register_final_tables():
        setup_hive(tables=FINAL_TABLES, database=FINAL_DATABASE, location_prefix="final", bucket=BUCKET)

    trips = build_trips()
    revenue = build_revenue_by_zone()
    weather = build_weather_impact()
    register = register_final_tables()

    [trips, revenue, weather] >> register


pipeline = final_pipeline()
