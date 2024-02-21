from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def scan(**kwargs):
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