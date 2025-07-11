from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import boto3
import pandas as pd
from catch_games_by_date import upload_to_s3


def fetch_league_data():
    url = 'https:''statsapi.mlb.com/api/v1/league'
    response = requests.get(url)
    data = response.json()

    df_league = pd.json_normalize(data, sep="_")

    # upload to s3
    date_str = datetime.today().strftime("%Y-%m-%d")
    upload_to_s3(df_league, bucket="selina-airflow", key=f"mlb/league/{date_str}.csv")

with DAG(
    dag_id="catch_league_data",
    start_date=datetime(2025, 7, 1),
    schedule_interval=False,
    catchup=False,
    tags=["etl"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_mlb_league_data",
        python_callable=fetch_league_data,
    )