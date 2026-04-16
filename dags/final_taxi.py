"""
# NYC Taxi Final Layer

Reads from the staging Hive tables (nyc_taxi.*) and produces three pre-aggregated
tables written to MinIO under final/ and registered in the nyc_taxi_final database.

| Table             | Description                                      |
|-------------------|--------------------------------------------------|
| trips_by_month    | Trip counts and avg fare per month/borough/type  |
| revenue_by_zone   | Revenue metrics per pickup zone and taxi type    |
| weather_impact    | Daily trips + avg fare joined with weather data  |

Run this DAG once after hive_setup completes, then keep it on monthly schedule
to refresh aggregations as new data arrives.
"""

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
import logging
from utils.default import get_default_args
from utils.s3 import file_exists
from utils.hive import setup_hive, FINAL_DATABASE
from utils.final.trips_by_month import compute_trips_by_month
from utils.final.revenue_by_zone import compute_revenue_by_zone
from utils.final.weather_impact import compute_weather_impact

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"

FINAL_TABLES = ["trips_by_month", "revenue_by_zone", "weather_impact"]


@dag(
    **get_default_args(
        dag_id="taxi_final",
        description="Monthly aggregation into the final layer (nyc_taxi_final)",
        schedule="@monthly",
        catchup=False,
        doc_md=__doc__,
    )
)
def final_pipeline():

    @task.sensor(task_id="wait_for_staging", mode="reschedule", timeout=3600, poke_interval=120)
    def wait_for_staging(logical_date=None) -> PokeReturnValue:
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")
        key = f"staging/yellow_taxi/partition_date={year}-{int(month):02d}-01/part-00000.parquet"
        return PokeReturnValue(is_done=file_exists(bucket=BUCKET, key=key))

    @task
    def build_trips_by_month():
        compute_trips_by_month(bucket=BUCKET)

    @task
    def build_revenue_by_zone():
        compute_revenue_by_zone(bucket=BUCKET)

    @task
    def build_weather_impact():
        compute_weather_impact(bucket=BUCKET)

    @task
    def register_final_tables():
        setup_hive(tables=FINAL_TABLES, database=FINAL_DATABASE, location_prefix="final")

    sensor = wait_for_staging()
    trips = build_trips_by_month()
    revenue = build_revenue_by_zone()
    weather = build_weather_impact()
    register = register_final_tables()

    sensor >> [trips, revenue, weather] >> register


pipeline = final_pipeline()
