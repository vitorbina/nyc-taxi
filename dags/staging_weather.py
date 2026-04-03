"""
# NYC Weather Staging

Reads raw weather JSON files from MinIO, converts hourly arrays into
row-per-hour parquet files with type casting and weather code descriptions,
then writes to the staging layer.

Runs daily, triggered after the raw weather ingestion DAG completes.
"""

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
import logging
from utils.default import get_default_args
from utils.s3 import file_exists
from utils.staging.weather import stage_weather
from utils.hive import repair_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"


@dag(
    **get_default_args(
        dag_id="weather_staging",
        description="Daily staging pipeline for NYC weather data",
        schedule="@daily",
        catchup=True,
        doc_md=__doc__,
    )
)
def weather_staging_pipeline():

    @task.sensor(task_id="wait_for_ingestion", mode="reschedule", timeout=3600, poke_interval=120)
    def wait_for_ingestion(data_interval_end=None) -> PokeReturnValue:
        date_str = data_interval_end.strftime("%Y-%m-%d")
        key = f"raw/weather/partition_date={date_str}/weather_nyc_{date_str}.json"
        return PokeReturnValue(is_done=file_exists(bucket=BUCKET, key=key))

    @task
    def stage_daily_weather(data_interval_end=None):
        date_str = data_interval_end.strftime("%Y-%m-%d")
        stage_weather(date_str=date_str, bucket=BUCKET)

    @task
    def update_hive():
        repair_table("weather")

    wait_for_ingestion() >> stage_daily_weather() >> update_hive()


pipeline = weather_staging_pipeline()
