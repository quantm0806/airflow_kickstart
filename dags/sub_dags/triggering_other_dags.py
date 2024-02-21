import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor


def _wait_for_abc():
    return False


def end():
    print("end")


with DAG(
        dag_id="trigger_dag",
        start_date=airflow.utils.dates.days_ago(3),
        schedule_interval="@daily",
        catchup=False
) as dag:
    polling = PythonSensor(
        task_id="polling",
        mode="reschedule",
        python_callable=_wait_for_abc
    )

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_dag_task",
        trigger_dag_id="triggered_dag"
    )

    polling >> trigger_dag


with DAG(
    dag_id="triggered_dag",
    schedule_interval=None
) as dag2:
    end = PythonOperator(
        task_id="end",
        python_callable=end,
        trigger_rule="none_failed"
    )