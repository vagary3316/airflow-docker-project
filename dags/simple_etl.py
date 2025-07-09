from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import boto3
import pandas as pd
from io import StringIO


def fetch_data():
    url = "https://api.sportsdata.io/v3/mlb/scores/json/AllTeams"
    api_key = Variables.get('mlb_api_key')

    headers = {
        "Ocp-Apim-Subscription-Key": api_key
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        teams = response.json()
        teams_df = pd.json_normalize(teams)

        upload_to_s3(teams_df, bucket="selina-airflow", key="teams_data.csv")
    else:
        print("Error", response.status_code)


def upload_to_s3(df, bucket,  key):
    """
    Upload a df as csv to S3
    :param df: The df wanna upload
    :param bucket: the bucket in s3
    :param key: filename that uploaded to s3
    :return: None
    """
    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

with DAG(
    dag_id="simple_etl",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_mlb_data",
        python_callable=fetch_data,
    )


