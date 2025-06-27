import os
import requests
import psycopg2
import json
from google.cloud import bigquery
from datetime import datetime
from utils_log import log_decorator
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv(override=True)

# Environment variables
GCP_PROJECT = os.getenv('GCP_PROJECT')
DATASET_ID = os.getenv('DATASET_ID')
TABLE_ID = os.getenv('TABLE_ID')

@log_decorator
def extract_city_weather_data(city: str, api_key: str) -> dict:
    '''
    Fetch raw weather data for a chosen city using OpenWeather API
    
    Parameters:
    city (str): The desired city you want to obtain the data
    api_key (str): OpenWeather API key
    
    Returns:
    dict: Weather data for the city
	'''
    api_url = 'https://api.openweathermap.org/data/2.5/weather'
    
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
	}
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        city_weather_data = response.json()
        
        return city_weather_data
        
    except requests.exceptions.RequestException as e:
        print(f'Error fetching data: {e}')

@log_decorator  
def transform_city_weather_data(data: dict) -> dict:
    '''
    Transform raw data into a more organized dictionary,
    selecting only the important informations, ready to
    load into database
    
    Parameters:
    data (dict): Raw weather data
    
    Returns:
    dict: Transformed weather data
	'''
    transformed_weather_data = {
        'timestamp': datetime.fromtimestamp(data['dt']).isoformat(),
        'city': data['name'],
        'temperature': round(data['main']['temp'], 2),
        'feels_like_temp': round(data['main']['feels_like'], 2),
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'description': data['weather'][0]['description'],
        'icon_url': f"https://openweathermap.org/img/wn/{data['weather'][0]['icon']}@2x.png",
        'longitude': data['coord']['lon'],
        'latitude': data['coord']['lat']
	}
    
    return transformed_weather_data

@log_decorator
def init_bigquery_table():
    '''Initialize BigQuery table if it does not exist'''
    client = bigquery.Client(project=GCP_PROJECT)

    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    try:
        client.get_table(table_ref)
        print(f'Table {GCP_PROJECT}.{DATASET_ID}.{TABLE_ID} already exists.')
    except Exception:
        print(f'Table does not exist, creating it...')

        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("temperature", "NUMERIC"),
            bigquery.SchemaField("feels_like_temp", "NUMERIC"),
            bigquery.SchemaField("humidity", "NUMERIC"),
            bigquery.SchemaField("wind_speed", "NUMERIC"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("icon_url", "STRING"),
            bigquery.SchemaField("longitude", "NUMERIC"),
            bigquery.SchemaField("latitude", "NUMERIC"),
            bigquery.SchemaField("created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")  

@log_decorator
def load_weather_data_to_bigquery(data: dict):
    '''
    Load the transformed weather data into BigQuery
    '''
    if not data:
        print('No data to load')
    
    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f'{GCP_PROJECT}.{DATASET_ID}.{TABLE_ID}'

    rows_to_insert = [data]

    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            print(f'BigQuery insert errors: {errors}')
        else:
            print(f'Sucessfully inserted data for {data["city"]}')
    except Exception as e:
        print(f'Error while inserting data to BigQuery: {e}')

@log_decorator
def test_bigquery_connection():
    '''Test BigQuery connection and permissions'''
    try: 
        client = bigquery.Client(project=GCP_PROJECT)

        query = f'''
        SELECT COUNT(*) AS row_count
        FROM `{GCP_PROJECT}.{DATASET_ID}.{TABLE_ID}`
        LIMIT 1
        '''

        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            print(f'BigQuery connection successful! Table has {row.row_count} rows')

    except Exception as e:
        print(f'BigQuery connection failed: {e}')


if __name__ == '__main__':
    test_bigquery_connection()
