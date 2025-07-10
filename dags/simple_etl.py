from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import boto3
import pandas as pd
from io import StringIO
import logging
import pytz


def fetch_data():
    # first catching the date in Pacific timezone
    pacific = pytz.timezone('US/Pacific')
    now_pacific = datetime.now(pacific)

    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
    params = {
        "date": now_pacific.strftime("%Y-%m-%d")
    }

    response = requests.get(url, params=params)
    data = response.json()

    games = data['dates'][0]['games']
    df_sch = pd.json_normalize(games, sep='_')

    # filter columns
    df_sch_clean = df_sch[[
        'gamePk',
        'gameDate',
        'officialDate',
        'gamesInSeries',
        'seriesGameNumber',
        'teams_home_team_id',
        'teams_home_team_name',
        'teams_home_score',
        'teams_away_team_name',
        'teams_away_score',
        'venue_name',
        'status_detailedState',
        'teams_home_leagueRecord_wins',
        'teams_away_leagueRecord_losses',
        'teams_home_leagueRecord_pct',
        'teams_away_leagueRecord_wins',
        'teams_away_leagueRecord_losses',
        'teams_home_leagueRecord_pct'
    ]]

    df_sch_clean.rename(columns={
        'gamePk': 'game_id',
        'gameDate': 'datetime_utc',
        'teams_home_team_name': 'home_team',
        'teams_home_score': 'home_score',
        'teams_away_team_name': 'away_team',
        'teams_away_score': 'away_score',
        'venue_name': 'venue',
        'status_detailedState': 'status'
    }, inplace=True)


    # upload to s3
    date_str = now_pacific.strftime("%Y-%m-%d")
    upload_to_s3(df_sch_clean, bucket="selina-airflow", key=f"mlb/schedule/{date_str}.csv")



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


