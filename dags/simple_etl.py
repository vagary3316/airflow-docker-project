
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv

def fetch_data():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()

    with open("/opt/airflow/dags/fake_data.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["userId", "id", "title", "body"])
        writer.writeheader()
        writer.writerows(data)

with DAG(
    dag_id="simple_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_fake_data",
        python_callable=fetch_data,
    )
