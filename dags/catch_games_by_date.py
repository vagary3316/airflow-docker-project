from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import pandas as pd
from io import StringIO
import sqlite3


def fetch_data():
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=14)
    url = "https://statsapi.mlb.com/api/v1/schedule?"

    params = {
        "sportId": 1,
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d")
    }
    response = requests.get(url, params=params)
    data = response.json()

    games = []
    for date_entry in data.get('dates', []):
        games.extend(date_entry.get('games', []))

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
        'teams_home_leagueRecord_losses',
        'teams_home_leagueRecord_pct',
        'teams_away_leagueRecord_wins',
        'teams_away_leagueRecord_losses',
        'teams_away_leagueRecord_pct'
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

    df_sch_clean['insert_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # upload to s3
    date_str = datetime.today().strftime("%Y-%m-%d")
    upload_to_s3(df_sch_clean, bucket="selina-airflow", key=f"mlb/schedule/{date_str}.csv")

    # store to sqlite
    conn = sqlite3.connect("mlb_data.db")
    df_sch_clean.to_sql("mlb_schedule", conn, if_exists="append", index=False)
    conn.close()

    # drop duplicates and check
    drop_duplicates_data('mlb_schedule')
    check_data('mlb_schedule')



def fetch_league_data():
    url = 'https://statsapi.mlb.com/api/v1/league'
    response = requests.get(url)
    data = response.json()

    df_league = pd.json_normalize(data['leagues'])
    df_league['insert_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # upload to s3
    date_str = datetime.today().strftime("%Y-%m-%d")
    upload_to_s3(df_league, bucket="selina-airflow", key=f"mlb/league/{date_str}.csv")


def fetch_player_data():
    url = 'https://statsapi.mlb.com/api/v1/sports/1/players'
    response = requests.get(url)
    data = response.json()

    df_player = pd.json_normalize(data['people'])
    df_team = df_player[['currentTeam.id', 'currentTeam.name']].drop_duplicates()
    df_team = df_team[
        df_team['currentTeam.name'].notna() &  # drop NaN
        (df_team['currentTeam.name'].str.strip() != '')  # drop empty strings
        ]
    df_team['insert_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    all_rosters = []
    for team_id in df_team['currentTeam.id']:
        data_roster = fetch_roster_data(team_id)  # this returns a DataFrame
        data_roster['team_id'] = team_id
        # optional: tag with team_id
        all_rosters.append(data_roster)  # add to list

    # Combine all into one DataFrame
    df_all_roster = pd.concat(all_rosters, ignore_index=True)
    df_all_roster['insert_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Merge team name for player data
    df_player = df_player.merge(df_team, left_on='currentTeam.id', right_on='currentTeam.id')

    # filter columns
    df_player_clean = df_player[[
        'id',
        'fullName',
        'link',
        'firstName',
        'lastName',
        'primaryNumber',
        'birthDate',
        'currentAge',
        'birthCity',
        'birthStateProvince',
        'birthCountry',
        'height',
        'weight',
        'boxscoreName',
        'mlbDebutDate',
        'strikeZoneTop',
        'strikeZoneBottom',
        'currentTeam.id',
        'primaryPosition.code',
        'primaryPosition.name',
        'primaryPosition.type',
        'primaryPosition.abbreviation',
        'batSide.code',
        'batSide.description',
        'pitchHand.code',
        'pitchHand.description',
        'nickName',
        'currentTeam.name_y'
    ]]

    df_player_clean.rename(columns={
        'currentTeam.name_y': 'Team'
    }, inplace=True)
    df_player_clean['insert_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # upload to s3
    date_str = datetime.today().strftime("%Y-%m-%d")
    upload_to_s3(df_player_clean, bucket="selina-airflow", key=f"mlb/player/{date_str}.csv")
    upload_to_s3(df_all_roster, bucket="selina-airflow", key=f"mlb/player/roster/{date_str}.csv")
    upload_to_s3(df_team, bucket="selina-airflow", key=f"mlb/team.csv")


def fetch_roster_data(teamId):
    url = f'https://statsapi.mlb.com/api/v1/teams/{teamId}/roster'
    response = requests.get(url)
    data = response.json()

    df_roster = pd.json_normalize(data['roster'])
    return df_roster


def upload_to_s3(df, bucket, key):
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


def check_data(table):
    # check table in sqlite
    conn = sqlite3.connect("mlb_data.db")
    cursor = conn.cursor()

    # Show all table names
    cursor.execute(f"SELECT * FROM {table};")
    tables = cursor.fetchall()

    print("Tables in database:")
    for row in tables:
        print(row)

    conn.close()

def drop_duplicates_data(table):
    conn = sqlite3.connect("mlb_data.db")
    cursor = conn.cursor()

    if table == 'mlb_schedule':
        delete_query = """
        WITH ranked AS (
            SELECT 
                rowid,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, datetime_utc
                    ORDER BY insert_date DESC, rowid DESC
                ) as rn
            FROM mlb_schedule
        )
        DELETE FROM mlb_schedule
        WHERE rowid IN (
            SELECT rowid FROM ranked WHERE rn > 1
        );
        """

        cursor.executescript(delete_query)
        conn.commit()
        print("Duplicates dropped from mlb_schedule")

    conn.close()


with DAG(
        dag_id="catch_games_by_date",
        start_date=datetime(2025, 7, 1),
        schedule_interval="0 8 * * *",
        catchup=False,
        tags=["etl"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_mlb_data",
        python_callable=fetch_data,
    )

# Second DAG
with DAG(
        dag_id="catch_league_data",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["etl"],
) as dag2:
    t2 = PythonOperator(
        task_id="fetch_mlb_league_data",
        python_callable=fetch_league_data,
    )

# Third DAG
with DAG(
        dag_id="catch_player_data",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["etl"],
) as dag3:
    t3 = PythonOperator(
        task_id="fetch_mlb_player_data",
        python_callable=fetch_player_data,
    )
