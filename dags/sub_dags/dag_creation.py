from airflow import DAG


def create_dag(dag_id: str, tags: list, **kwargs):
    generated_dag = DAG(dag_id, tags=tags, **kwargs)

    return generated_dag
