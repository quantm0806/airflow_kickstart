import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming",
    dag=dag,
)


def _get_pictures(**kwargs):
    execution_date = kwargs["execution_date"]
    print('execution date', execution_date)

    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
    image_urls = [launch["image"] for launch in launches["results"]]
    for image_url in image_urls:
        try:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"/tmp/images/{image_filename}"
            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {image_url} to {target_file}")
        except requests_exceptions.MissingSchema:
            print(f"{image_url} appears to be an invalid URL.")
        except requests_exceptions.ConnectionError:
            print(f"Could not connect to {image_url}.")

    next_execution_date = kwargs["next_execution_date"]
    print('next execution date', next_execution_date)



get_pictures = PythonOperator(
    task_id="get_pictures",
    provide_context=True,
    python_callable=_get_pictures,
    dag=dag,
)

notify = EmailOperator(
    task_id="send_email",
    to='quan.tm0806@gmail.com',
    subject='Rocket launches information',
    html_content=f"Hello world",
    dag=dag
)

download_launches >> get_pictures >> notify
