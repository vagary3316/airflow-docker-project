
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("👋 Hello, Airflow is working!")

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo"],
) as dag:
    t1 = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )
