"""
# NYC Weather Staging

Reads raw weather JSON files from MinIO, converts the hourly arrays into
row-per-hour parquet files, and writes to the staging layer.

Runs daily, triggered after the raw weather ingestion DAG completes.
"""

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import logging
from utils.default import get_default_args
from utils.staging.weather import stage_weather

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"


@dag(
    **get_default_args(
        dag_id="nyc_weather_staging",
        description="Daily staging pipeline for NYC weather data",
        schedule="@daily",
        catchup=True,
        dag_file=__file__,
        doc_md=__doc__,
    )
)
def weather_staging_pipeline():

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="nyc_weather_ingestion",
        external_task_id=None,
        mode="reschedule",
        timeout=3600,
    )

    @task
    def stage_daily_weather(data_interval_end=None):
        date_str = data_interval_end.strftime("%Y-%m-%d")
        stage_weather(date_str=date_str, bucket=BUCKET)

    wait_for_ingestion >> stage_daily_weather()


pipeline = weather_staging_pipeline()
