import datetime

from airflow.decorators import task, dag
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


@dag(
    dag_id='nebula_task_flow',
    start_date=datetime.datetime(2024, 1, 1),
    schedule_interval=None
)
def run_pipeline():
    @task
    def scan():
        try:
            hook = MsSqlHook(mssql_conn_id="airflow_mssql")
            sql = """
                    select QuestionID, ClassName from Questions
                    where QuestionID = '63a157de96943b6dd8524347';
                """
            result = hook.get_records(sql)
            return result
        except Exception as e:
            print("Error connecting to MongoDB -- {e}")

    @task
    def process(scan_result):
        print('question id: ', scan_result[0][0])
        print('class name: ', scan_result[0][1] + 'hello world')

    scan_result = scan()
    process(scan_result)


run_pipeline()
