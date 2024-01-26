FROM apache/airflow:latest-python3.9
ADD requirements.txt .
RUN pip install -r requirements.txt