import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# Set up the page
st.set_page_config(page_title="SkyFlow Dashboard", layout="wide")
st.title("🌤️ SkyFlow Weather Analytics")

# Database Connection
def get_data():
    conn = psycopg2.connect(
        host="localhost", port="5433", 
        database="weather_data", user="admin", password="password123"
    )
    # Query the GOLD VIEW we made!
    query = "SELECT * FROM daily_weather_summary"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    df = get_data()

    # Metrics Row
    col1, col2, col3 = st.columns(3)
    latest = df.iloc[0]
    col1.metric("City", latest['city'])
    col2.metric("Avg Temp", f"{latest['avg_temp']}°C")
    col3.metric("Max Temp", f"{latest['max_temp']}°C")

    # Chart
    st.subheader("Temperature Trends")
    fig = px.line(df, x='report_date', y='avg_temp', title="Daily Average Temperature")
    st.plotly_chart(fig, use_container_width=True)

    # Raw Data Table
    st.subheader("Cleaned Data (Silver Layer)")
    st.dataframe(df)

except Exception as e:
    st.error(f"Waiting for more data... Error: {e}")