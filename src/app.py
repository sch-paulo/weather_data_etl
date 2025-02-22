import os
import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": "5432" 
}

@st.cache_data(ttl=60)
def get_data(city=None):
    """Fetch data from PostgreSQL with caching"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT * FROM weather_capitals"  # Match your table name
        if city and city != 'All':
            query += f" WHERE city = '{city}'"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

@st.cache_data(ttl=60)
def get_latest_weather():
    """Get latest weather entry for each city"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            WITH latest_data AS (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY city ORDER BY timestamp DESC) as rn
                FROM weather_capitals
            )
            SELECT * FROM latest_data WHERE rn = 1
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

# Main app
st.title("Weather Dashboard 🌦️")

# Sidebar filters
st.sidebar.header("Filters")
df = get_data()
cities = ['All'] + df['city'].unique().tolist()
selected_city = st.sidebar.selectbox("Select City", cities)

# Refresh button
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()

# Get filtered data
df_filtered = get_data(selected_city if selected_city != 'All' else None)

# Show raw data
if st.checkbox("Show Raw Data"):
    st.dataframe(df_filtered)

# Key metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Records", len(df_filtered))
col2.metric("Cities Covered", df_filtered['city'].nunique())
col3.metric("Avg Temperature", f"{df_filtered['temperature'].mean():.1f}°C")

# Temperature chart
st.subheader("Temperature Over Time")
if not df_filtered.empty:
    st.line_chart(df_filtered.set_index('timestamp')['temperature'])

# Weather map
# Replace the original map section with this:
st.subheader("Live Weather Map")
map_data = get_latest_weather()

if not map_data.empty:
    # Create a Folium map with custom icons
    m = folium.Map(location=[map_data['latitude'].mean(), 
                  map_data['longitude'].mean()],
                  tiles="Cartodb dark_matter",
                  zoom_start=4)

    for _, row in map_data.iterrows():
        popup_html = f"""
        <div style="text-align: center;">
            <b>{row['city']}</b><br>
            {row['temperature']}°C<br>
            {row['description'].title()}
        </div>
        """
        
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=200),
            icon=folium.features.CustomIcon(
                row['icon_url'],
                icon_size=(40, 40),
                icon_anchor=(20, 20)
            )
        ).add_to(m)

    # Display the map in Streamlit
    st_folium(m, use_container_width=True)

# Weather stats
st.subheader("Weather Conditions")
if not df_filtered.empty:
    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(df_filtered['description'].value_counts())
    with col2:
        st.write("Wind Speed Distribution")
        st.area_chart(df_filtered['wind_speed'])

