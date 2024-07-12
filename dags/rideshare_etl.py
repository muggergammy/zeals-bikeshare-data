from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 07, 12),
    'catchup': False
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bikeshare_etl',
    default_args=default_args,
    description='A simple ETL pipeline for Bikeshare data',
    schedule_interval='5 12 * * *',
)

def extract_data():
    os.system("python /scripts/bikeshare_extract.py")

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

extract_task
