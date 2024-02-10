import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator


path = os.path.expanduser('~/final_work')
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.load import load

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 2, 5),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='load_new_data',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )
    load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    pipeline >> load
