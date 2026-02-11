import sys
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from dotenv import load_dotenv


root_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
load_dotenv(os.path.join(root_folder, '.env'))

sys.path.insert(0, os.path.join(root_folder, 'scripts'))

from ingestion import run_ingestion

@dag(
    dag_id='nyc_taxi_ingestion',
    description='Pipeline Igestion NYC Taxi (TaskFlow)',
    schedule='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'owner': 'vitor', 'retries': 1}
)
def pipeline_ingestao():

    @task
    def perform_ingestion(**context):
        data = context['logical_date']
        
        print(f"🚀 Iniciando ingestão para {data.strftime('%Y-%m')}")

        run_ingestion(
            year=data.strftime("%Y"),
            month=data.strftime("%m"),
            bucket_name="nyc-taxi-lake"
        )

    perform_ingestion()

pipeline = pipeline_ingestao()