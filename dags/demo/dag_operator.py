import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from demo.processor.processor import process
from demo.scanner.scanner import scan


@dag(
    dag_id='nebula_operator',
    start_date=datetime.datetime(2024, 1, 1),
    schedule_interval=None
)
def run_pipeline():
        scanner = PythonOperator(
            task_id="scanner",
            python_callable=scan
        )

        processor = PythonOperator(
            task_id="processor",
            python_callable=process
        )

        scanner >> processor


run_pipeline()
