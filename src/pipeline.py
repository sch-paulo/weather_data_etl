
import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Environment variables
API_KEY = os.getenv('API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASS')


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
        'appid': api_key
	}
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        city_weather_data = response.json()
        
        return city_weather_data
        
    except requests.exceptions.RequestException as e:
        print(f'Error fetching data: {e}')
        
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
        'timestamp': datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S'),
        'city': data['name'],
        'temperature': round(data['main']['temp'] - 273.15, 2),
        'feels_like_temp': round(data['main']['feels_like'] - 273.15, 2),
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'description': data['weather'][0]['description'],
        'longitude': data['coord']['lon'],
        'latitude': data['coord']['lat']
	}
    
    return transformed_weather_data

def database_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_database():
    conn = None
    try:
        conn = database_connection()
        cur = conn.cursor()

        with open('schema.sql', 'r') as f:
            cur.execute(f.read())

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f'Error while initializing database: {e}')
    finally:
        if conn is not None:
            conn.close()

        

def load_weather_data_on_database(data: dict):
    '''
    Load the transformed weather data into the
    connected database
    
    Parameters:
    data (dict): Transformed weather data
	'''
    sql_insert_query = """
        INSERT INTO weather_capitals
        (timestamp, city, temperature, feels_like_temp, 
        humidity, wind_speed, description, longitude, latitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    conn = None
    try:
        conn = database_connection()
        cur = conn.cursor()
        cur.execute(sql_insert_query, (
            data['timestamp'],
            data['city'],
            data['temperature'],
            data['feels_like_temp'],
            data['humidity'],
            data['wind_speed'],
            data['description'],
            data['longitude'],
            data['latitude']
        ))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f'Error while inserting data: {e}')
    finally:
        if conn is not None:
            conn.close()

    
if __name__ == '__main__':
    init_database()
    data = extract_city_weather_data('Manaus', API_KEY)
    transformed_data = transform_city_weather_data(data)
    load_weather_data_on_database(transformed_data)
