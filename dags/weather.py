from datetime import datetime
from airflow.decorators import dag, task
import logging
from utils.default import get_default_args
from utils.weather import ingest_weather_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dag_doc_md = """
# NYC Weather Ingestion

Fetches daily historical weather data for New York City from the Open-Meteo Archive API.
Source: Open-Meteo (free, no API key). Destination: MinIO bucket `data-lake-nyc`. Runs daily.

Partition: `raw/weather/partition_date=YYYY-MM-DD/`
"""

@dag(
    **get_default_args(
        dag_id='nyc_weather_ingestion',
        description='Daily weather data ingestion pipeline (Open-Meteo Archive API)',
        schedule='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        doc_md=dag_doc_md,
    )
)
def weather_ingestion_pipeline():

    @task
    def ingest_daily_weather(data_interval_end=None):
        ingest_weather_data(
            execution_date=data_interval_end,
            bucket="data-lake-nyc"
        )

    ingest_daily_weather()

pipeline = weather_ingestion_pipeline()