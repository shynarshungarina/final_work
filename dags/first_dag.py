import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/final_work')
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)

from modules.collect_data import collect_data
from modules.load_data import load_data

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 2, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='load_sber_data',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    collect_data = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
    )
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    collect_data >> load_data