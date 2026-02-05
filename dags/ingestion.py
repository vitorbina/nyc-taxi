import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from dateutil.relativedelta import relativedelta

cutoff_date = datetime.now().replace(day=1) - relativedelta(months=4)

PROJECT_PATH = "/home/vitor/projects/nyc-taxi"

default_args = {
    'owner': 'vitor',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    dag_id= 'nyc_taxi_ingestion',
    default_args=default_args,
    start_date=datetime(2020,1,1),
    end_date= cutoff_date,
    schedule_interval='@monthly',
    description='Monthly ingestion for NYC Taxi Data',
    catchup=True,
    max_active_runs=3,
    tags = ['nyc_taxi', 'ingestion'],
) as dag:
    ingest_task = BashOperator(
        task_id='ingest_monthly_data',
        bash_command=f"""
        source ~/miniconda3/etc/profile.d/conda.sh && \
        conda activate projeto-dados && \
        python {PROJECT_PATH}/scripts/ingestion_script.py \
        --year {{ logical_date.strftime('%Y') }} \
        --month {{ logical_date.strftime('%m') }}
        """
    )
