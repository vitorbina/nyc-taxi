#Returns the default arguments to be used across all DAGs in the project.
from datetime import timedelta

def get_default_args():
    return {
        'owner': 'Vitor Bina',
        'depends_on_past': False,
        'email': ['vitor.bina10@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }