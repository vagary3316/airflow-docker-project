import streamlit as st


## streamlit_setting
st.set_page_config(layout="wide",
                   page_title="Data Visualization for MLB data",
                   page_icon=":baseball:")
# Streamlit Header and Subheader
st.header('Data Visualization for MLB data')

# Initialize connection.
conn = st.connection("sql")

# Perform query.
test_df = conn.query('SELECT * from league;', ttl=600)

st.dataframe(test_df)