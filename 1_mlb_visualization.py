import streamlit as st

## streamlit_setting
st.set_page_config(layout="wide",
                   page_title="Data Visualization for MLB data",
                   page_icon=":baseball:")
# Streamlit Header and Subheader
st.header('Data Visualization for MLB data')

# Initialize connection.
conn = st.connection("my_mysql", type="sql")

# Perform query.
test_df = conn.query('''
SELECT officialDate,
       datetime_utc,
       seriesGameNumber,
       home_team,
       home_score,
       away_team,
       away_score
from mlb_schedule
where officialDate = (SELECT MAX(officialDate) from mlb_schedule WHERE status = 'Final')
and status = 'Final';
'''
                     , ttl=600)

st.dataframe(test_df)
