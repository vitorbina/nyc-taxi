"""
# NYC Weather Curated

Reads staged weather parquet files from MinIO, translates WMO weather codes
to human-readable descriptions, and writes to the curated layer.

Runs daily, after the staging weather DAG completes.
"""

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import logging
from utils.default import get_default_args
from utils.curated.weather import curate_weather

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "data-lake-nyc"


@dag(
    **get_default_args(
        dag_id="nyc_weather_curated",
        description="Daily curated pipeline for NYC weather data",
        schedule="@daily",
        catchup=True,
        dag_file=__file__,
        doc_md=__doc__,
    )
)
def weather_curated_pipeline():

    wait_for_staging = ExternalTaskSensor(
        task_id="wait_for_staging",
        external_dag_id="nyc_weather_staging",
        external_task_id=None,
        mode="reschedule",
        timeout=3600,
    )

    @task
    def curate_daily_weather(data_interval_end=None):
        date_str = data_interval_end.strftime("%Y-%m-%d")
        curate_weather(date_str=date_str, bucket=BUCKET)

    wait_for_staging >> curate_daily_weather()


pipeline = weather_curated_pipeline()
