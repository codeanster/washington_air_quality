import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
import os 
from dotenv import load_dotenv

load_dotenv()  # This loads the variables from a .env file

# Function to get data from the PostgreSQL database
def get_data():
    conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
    )

    query = 'SELECT * FROM import_air_quality'
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Streamlit app configuration
st.set_page_config(page_title='Air Quality Dashboard', page_icon=':earth_americas:', layout='wide')

# Custom CSS for styling
st.markdown("""
    <style>
    .main {
        background-color: #f0f2f6;
        color: #1c1c1c;
        font-family: 'Arial', sans-serif;
    }
    .title {
        text-align: center;
        color: #4CAF50;
    }
    </style>
    """, unsafe_allow_html=True)

# Title
st.markdown("<h1 style='text-align: center; color: #4CAF50;'>Air Quality Dashboard</h1>", unsafe_allow_html=True)

# Cache the data loading to avoid repeated queries to the database
@st.cache_data(ttl=1800)  # Cache data for 30 minutes
def load_data():
    return get_data()

data = load_data()

# Ensure the data is sorted by report_date
data = data.sort_values(by='report_date')

# Interpolate missing data if necessary
data = data.interpolate()

# Dropdown for location selection
all_locations = data['location'].unique()

# Input fields
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    selected_location = st.selectbox("Location:", all_locations)
with col2:
    date = st.date_input("Date:")
with col3:
    time = st.time_input("Time:")

filtered_data = data[data['location'] == selected_location]

# Display filtered data
st.sidebar.write('## Filtered Air Quality Data')
st.sidebar.dataframe(filtered_data)

# Displaying PM2.5
st.write(" ")
col4, col5 = st.columns([1, 1])
with col4:
    st.text_input("PM2.5:", value=str(filtered_data['air_quality_pm25'].max()))
with col5:
    st.text_input("Last Update:", value=str(filtered_data['last_update'].max()))

# Combine the three scatter plots into one plot
def plot_combined_scatter(data):
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(x=data['report_date'], y=data['air_quality_pm25'],
                            mode='lines+markers', name='PM2.5', marker=dict(color='red')))
    fig.add_trace(go.Scatter(x=data['report_date'], y=data['air_quality_pm10'],
                            mode='lines+markers', name='PM10', marker=dict(color='blue')))
    fig.add_trace(go.Scatter(x=data['report_date'], y=data['air_quality_ozone'],
                            mode='lines+markers', name='Ozone', marker=dict(color='green')))
    fig.update_layout(
        title='Air Quality Over Time',
        xaxis_title='Report Date',
        yaxis_title='Levels',
        autosize=False,
        width=1200,  # Set the width you want
        height=800,  # Set the height you want
        title_x=0.5,
        title_font=dict(size=24, color='#4CAF50'),
        font=dict(family="Arial, sans-serif", size=14, color="#1c1c1c"),
        plot_bgcolor='#f0f2f6',
        paper_bgcolor='#f0f2f6',
    )
    st.plotly_chart(fig, use_container_width=True)

# Render the combined plot
plot_combined_scatter(filtered_data)

# Get the most recent data
most_recent_date = data['report_date'].max()
recent_data = data[data['report_date'] == most_recent_date]

# Get the top 5 most polluted locations based on air_quality_pm25
top_5_polluted = recent_data.nlargest(5, 'air_quality_pm25')

# Add clickable buttons for each top location
st.write("## Top 5 Most Polluted Locations (PM2.5)")

for location in top_5_polluted['location']:
    if st.button(f"View data for {location}"):
        selected_location = location
        filtered_data = data[data['location'] == selected_location]
        plot_combined_scatter(filtered_data)

# Plot the most polluted locations
fig = px.bar(top_5_polluted, x='location', y='air_quality_pm25', title='Top 5 Most Polluted Locations (PM2.5)',
             color='location', text='air_quality_pm25')
fig.update_layout(
    autosize=False,
    width=1200,  # Set the width you want
    height=800,  # Set the height you want
    title={
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 24, 'color': '#4CAF50'}
    },
    xaxis_title='Location',
    yaxis_title='PM2.5 Level',
    font=dict(
        family="Arial, sans-serif",
        size=14,
        color="#1c1c1c"
    ),
    plot_bgcolor='#f0f2f6',
    paper_bgcolor='#f0f2f6',
)
fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')

st.plotly_chart(fig, use_container_width=True)

st.write("")
st.write("""
    <footer style="text-align: center; color: #1c1c1c;">
        © 2024 Environmental Company. All rights reserved.
    </footer>
    """, unsafe_allow_html=True)
