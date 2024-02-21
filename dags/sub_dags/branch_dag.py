import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


def branch():
    x = 1
    if x == 1:
        return "task_1"
    else:
        return "task_2"


def task_1():
    print("1")


def task_2():
    print("2")


def end():
    print("end")


dag = DAG(dag_id="branch_dag", tags=["abc"], start_date=airflow.utils.dates.days_ago(3),
                 schedule_interval="@daily",
                 catchup=False)

with dag:
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=branch,
    )

    task_1_dag = PythonOperator(
        task_id="task_1",
        python_callable=task_1,
    )

    task_2_dag = PythonOperator(
        task_id="task_2",
        python_callable=task_2,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=end,
        trigger_rule="none_failed"
    )

    branch >> [task_1_dag, task_2_dag] >> end
