import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator


path = os.path.expanduser('~/final_work')
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)

from modules.collect_new import collect_new
from modules.load_new import load_new

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 2, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='load_new_data',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    collect_new = PythonOperator(
        task_id='collect_new',
        python_callable=collect_new,
    )
    load_new = PythonOperator(
        task_id='load_new',
        python_callable=load_new,
    )

    collect_new >> load_new
