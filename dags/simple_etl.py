from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import boto3
import pandas as pd
from io import StringIO
import logging


def fetch_data():
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
    response = requests.get(url)

    data = response.json()
    df_sch = pd.json_normalize(data)

    upload_to_s3(df_sch, bucket="selina-airflow", key="schedule.csv")



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


