import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
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
    start_date=datetime(2020,1,1),
    schedule='@monthly',
    catchup=False,
    tags = ['nyc_taxi', 'ingestion'],
) as dag:
    ingest_task = BashOperator(
        task_id='ingest_monthly_data',
bash_command="""source ~/miniconda3/etc/profile.d/conda.sh && \
conda activate projeto-dados && \
python /home/vitor/projects/nyc-taxi/scripts/ingestion_script.py \
--year {{ ds_nodash[:4] }} \
--month {{ ds_nodash[4:6] }}"""
)
