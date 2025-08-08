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
test_df = conn.query('SELECT * from all_teams_prob;', ttl=600)

st.dataframe(test_df)