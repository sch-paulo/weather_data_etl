import streamlit as st
import pandas as pd
from google.cloud import bigquery

# --- Page Configuration ---
st.set_page_config(
    page_title="Brazilian Capitals Weather Dashboard",
    page_icon="🇧🇷",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- GCP Authentication & BigQuery Client ---
PROJECT_ID = "weather-data-etl-464123"
try:
    client = bigquery.Client(project=PROJECT_ID)
    st.sidebar.success(f"Connected to BigQuery Project: {PROJECT_ID}", icon="☁️")
except Exception as e:
    st.error(f"Failed to connect to BigQuery. Please check your authentication. Error: {e}")
    st.stop()


# --- Caching Data Loading Function ---
@st.cache_data(ttl=900)
def run_query(query: str) -> pd.DataFrame:
    """Runs a BigQuery query and returns the results as a Pandas DataFrame."""
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"An error occurred while running the query: {e}")
        return pd.DataFrame()

# --- Load Data from dbt Marts ---
df_latest_weather = run_query("SELECT * FROM `weather-data-etl-464123.weather_marts.dim_weather_latest`")
df_forecasts = run_query("SELECT * FROM `weather-data-etl-464123.weather_data.weather_forecasts`")
df_accuracy = run_query("SELECT * FROM `weather-data-etl-464123.weather_marts.fact_forecast_accuracy` ORDER BY forecast_for_date")

if df_latest_weather.empty:
    st.error("Could not load latest weather data. Please check if the dbt models have run successfully.")
    st.stop()

# --- Sidebar for City Selection ---
st.sidebar.title("City Selection")
sorted_cities = sorted(df_latest_weather['city'].unique())
selected_city = st.sidebar.selectbox(
    "Select a Brazilian Capital:",
    sorted_cities,
    index=sorted_cities.index("São Paulo") if "São Paulo" in sorted_cities else 0 # Default to São Paulo
)

# Filter data for the selected city
city_weather = df_latest_weather[df_latest_weather['city'] == selected_city].iloc[0]
city_forecasts = df_forecasts[df_forecasts['city'] == selected_city]
city_accuracy = df_accuracy[df_accuracy['city'] == selected_city]


# --- Main Dashboard ---
st.title(f"Weather Dashboard: {selected_city}")

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["Current Conditions", "Future Forecast", "Forecast Accuracy"])

# --- Tab 1: Current Conditions ---
with tab1:
    st.header(f"Latest Observation for {selected_city}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Temperature", f"{city_weather['temp_celsius']} °C")
    col2.metric("Feels Like", f"{city_weather['feels_like_celsius']} °C")
    col3.metric("Humidity", f"{city_weather['humidity']}%")
    col4.metric("Wind Speed", f"{city_weather['wind_speed']} m/s")

    st.subheader("Map of All Capitals")
    # We need to rename the columns for st.map to work
    map_data = df_latest_weather.rename(columns={'latitude': 'lat', 'longitude': 'lon'})
    st.map(map_data[['lat', 'lon']], zoom=3)
    st.caption(f"Last updated at: {pd.to_datetime(city_weather['observed_at_utc']).strftime('%Y-%m-%d %H:%M')} UTC")


# --- Tab 2: Future Forecast ---
with tab2:
    st.header("Temperature Forecast")
    if not city_forecasts.empty:
        # Prepare data for charting
        forecast_chart_data = city_forecasts[['forecast_for_date', 'predicted_temp_celsius']].copy()
        forecast_chart_data = forecast_chart_data.set_index('forecast_for_date')
        
        st.line_chart(forecast_chart_data)
        st.write("Forecast Data:")
        st.dataframe(city_forecasts[['forecast_for_date', 'predicted_temp_celsius', 'predicted_description', 'source']])
    else:
        st.warning("No forecast data available for this city.")


# --- Tab 3: Forecast Accuracy ---
with tab3:
    st.header("Forecast vs. Actual Temperature")
    if not city_accuracy.empty:
        # Prepare data for charting
        accuracy_chart_data = city_accuracy.rename(columns={
            'predicted_temp_celsius': 'Predicted',
            'actual_avg_temp_celsius': 'Actual'
        }).set_index('forecast_for_date')[['Predicted', 'Actual']]

        st.line_chart(accuracy_chart_data)

        # Calculate and display Mean Absolute Error (MAE)
        mae = abs(city_accuracy['temp_error_celsius']).mean()
        st.metric("Mean Absolute Error (MAE)", f"{mae:.2f} °C", help="The average absolute difference between the predicted and actual temperature.")
        
        st.write("Accuracy Details:")
        st.dataframe(city_accuracy[['forecast_for_date', 'Predicted', 'Actual', 'temp_error_celsius']])
    else:
        st.warning("No forecast accuracy data available to display. This requires both forecasts and actual observations for the same day.")

# --- About Section in Sidebar ---
st.sidebar.markdown("---")
st.sidebar.info(
    """
    This dashboard is the final product of a complete ETL/ELT data pipeline built on Google Cloud Platform using only free-tier services.
    - **Orchestration**: Cloud Scheduler
    - **Data Ingestion**: Cloud Functions
    - **Transformation**: dbt Core
    - **Data Warehouse**: BigQuery
    - **Dashboard**: Streamlit on Cloud Run
    """
)
