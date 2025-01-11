
import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load ambient variables from .env
load_dotenv()

API_KEY = os.getenv('API_KEY')

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
        'city': data['name'],
        'temperature': round(data['main']['temp'] - 273.15, 2),
        'feels_like_temp': round(data['main']['feels_like'] - 273.15, 2),
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'description': data['weather'][0]['description'],
        'longitude': data['coord']['lon'],
        'latitude': data['coord']['lat'],
        'timestamp': datetime.fromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
	}
    
    return transformed_weather_data

# def load_weather_data_on_postgres(data: dict):
     
    
if __name__ == '__main__':
		
	data = extract_city_weather_data('Manaus', API_KEY)
	# print(data)
	transformed_data = transform_city_weather_data(data)
	print(transformed_data)

# {
#     'coord': {
#         'lon': -46.6361, 'lat': -23.5475
#         }, 
#     'weather': [{
#         'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'
#         }], 
#     'base': 'stations', 
#     'main': {
#         'temp': 297.64, 'feels_like': 297.63, 'temp_min': 296.9, 'temp_max': 298.4, 'pressure': 1011, 'humidity': 57, 'sea_level': 1011, 'grnd_level': 923
#         }, 
#     'visibility': 10000, 
#     'wind': {
#         'speed': 2.24, 'deg': 285, 'gust': 2.24
#         }, 
#     'clouds': {
#         'all': 0
#         }, 
#     'dt': 1736606255, 
#     'sys': {
#         'type': 1, 'id': 8394, 'country': 'BR', 'sunrise': 1736584218, 'sunset': 1736632701
#         }, 
#     'timezone': -10800, 
#     'id': 3448439, 
#     'name': 'São Paulo', 
#     'cod': 200}
