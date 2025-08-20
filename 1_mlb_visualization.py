import streamlit as st

## streamlit_setting
st.set_page_config(layout="wide",
                   page_title="Data Visualization for MLB data",
                   page_icon=":baseball:")
# Streamlit Header and Subheader
st.header('Data Visualization for MLB data')

# Initialize connection
conn = st.connection("my_mysql", type="sql")

# Streamlit Header and Subheader
st.subheader('Schedule and Scores')

# Table of Schedule
schedule_df = conn.query('''
    SELECT officialDate, datetime_utc, seriesGameNumber, home_team,
            home_score, away_team,away_score,teams_home_leagueRecord_wins,
            teams_home_leagueRecord_losses, teams_away_leagueRecord_wins,
            teams_away_leagueRecord_losses
    from mlb_schedule
    where officialDate = (SELECT MAX(officialDate) from mlb_schedule WHERE status = 'Final')
    and status = 'Final';
'''
                         , ttl=600)

schedule_df['teams'] = schedule_df['away_team'] + " @ " + schedule_df['home_team']
schedule_df['scores'] = schedule_df['away_score'] + "-" + schedule_df['home_score']
schedule_df = schedule_df[['officialDate', 'datetime_utc', 'teams', 'scores']]

st.dataframe(schedule_df)
