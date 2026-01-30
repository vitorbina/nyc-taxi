from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from dateutil.relativedelta import relativedelta

data_limite = datetime.now().replace(day=1) - relativedelta(months=4)

default_args = {
    'owner': 'vitor',
    'depends_on_past': False,
}

with DAG(
    dag_id= 'ingestao_nyc_taxi',
    default_args=default_args,
    start_date=datetime(2020,1,1),
    end_date= data_limite,
    schedule_interval='@monthly',
    description='Ingestao de dados de taxi NYC',
    catchup=True,
    max_active_runs=3,
    tags = ['projeto_ponta_a_ponta'],
) as dag:
    tarefa_ingestao = BashOperator(
        task_id='Ingestao_taxi_NYC',
        bash_command="""
        source ~/miniconda3/etc/profile.d/conda.sh && \
        conda activate projeto-dados && \
        python ~/projects/projeto-ponta-ponta/scripts/ingestion.py \
        --ano {{ logical_date.strftime('%Y') }} \
        --mes {{ logical_date.strftime('%m') }}
        """
    )