import functions_framework
import os
import requests
from google.cloud import bigquery
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_city_weather_data(city: str, api_key: str) -> dict:
    """Extract weather data from OpenWeatherMap API"""
    api_url = 'https://api.openweathermap.org/data/2.5/weather'
    
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric' 
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully fetched data for {city}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f'Error fetching data for {city}: {e}')
        return None

def transform_city_weather_data(data: dict) -> dict:
    """Transform raw weather data"""
    if not data:
        return None
        
    try:
        transformed = {
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
        return transformed
    except KeyError as e:
        logger.error(f"Missing key in weather data: {e}")
        return None

def load_to_bigquery(data: dict, project_id: str) -> bool:
    """Load data to BigQuery"""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.weather_data.weather_capitals"
        
        rows = [data]
        errors = client.insert_rows_json(table_id, rows)
        
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            return False
        
        logger.info(f"Successfully loaded data for {data['city']}")
        return True
        
    except Exception as e:
        logger.error(f"BigQuery error: {e}")
        return False

@functions_framework.http
def get_current_weather(request):
    """Main ETL function triggered by HTTP request"""
    
    # Brazilian capitals
    cities = [
        "Aracaju", "Belém", "Belo Horizonte", "Boa Vista", "Brasília", 
        "Campo Grande", "Cuiabá", "Curitiba", "Florianópolis", "Fortaleza", 
        "Goiânia", "João Pessoa", "Macapá", "Maceió", "Manaus", "Natal", 
        "Palmas", "Porto Alegre", "Porto Velho", "Recife", "Rio Branco", 
        "Rio de Janeiro", "Salvador", "São Luís", "São Paulo", "Teresina", "Vitória"
    ]
    
    # Get environment variables
    api_key = os.getenv('OPENWEATHER_API_KEY')
    project_id = os.getenv('GCP_PROJECT')
    
    if not api_key:
        return {'error': 'OPENWEATHER_API_KEY not set'}, 400
    
    if not project_id:
        return {'error': 'GCP_PROJECT not set'}, 400
    
    results = {
        'total_cities': len(cities),
        'successful': 0,
        'failed': 0,
        'errors': []
    }
    
    for city in cities:
        try:
            # Extract
            raw_data = extract_city_weather_data(city, api_key)
            
            if raw_data:
                # Transform
                clean_data = transform_city_weather_data(raw_data)
                
                if clean_data:
                    # Load
                    if load_to_bigquery(clean_data, project_id):
                        results['successful'] += 1
                    else:
                        results['failed'] += 1
                        results['errors'].append(f"Failed to load {city} to BigQuery")
                else:
                    results['failed'] += 1
                    results['errors'].append(f"Failed to transform data for {city}")
            else:
                results['failed'] += 1
                results['errors'].append(f"Failed to extract data for {city}")
                
        except Exception as e:
            results['failed'] += 1
            results['errors'].append(f"Unexpected error for {city}: {str(e)}")
            logger.error(f"Unexpected error processing {city}: {e}")
    
    logger.info(f"ETL completed: {results['successful']}/{results['total_cities']} successful")
    return results


# FORECAST ETL

def get_weatherapi_data(city: str, api_key: str) -> dict:
    """Extract 3-day forecast data from WeatherAPI.com"""
    api_url = 'https://api.weatherapi.com/v1/forecast.json'
    
    params = {
        'key': api_key,
        'q': city,
        'days': 3,
        'aqi': 'no',
        'alerts': 'no'
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully fetched forecast for {city} from WeatherAPI")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f'Error fetching forecast for {city}: {e}')
        return None

def transform_weatherapi_forecast(data: dict) -> dict:
    """Transforms raw WeatherAPI forecast data for the last available day."""
    if not data or 'forecast' not in data:
        return None
    
    try:
        last_day_forecast = data['forecast']['forecastday'][-1]
        city_name = data['location']['name']
        forecast_date = last_day_forecast['date']

        transformed = {
            'forecast_id': f"{city_name.lower().replace(' ','_')}-weatherapi-{forecast_date}",
            'city': city_name,
            'source': 'weatherapi',
            'forecast_made_at': datetime.now().isoformat(),
            'forecast_for_date': forecast_date,
            'predicted_temp': round(last_day_forecast['day']['avgtemp_c'], 2),
            'predicted_description': last_day_forecast['day']['condition']['text'],
        }
        return transformed
    except (KeyError, IndexError) as e:
        logger.error(f"Missing key or index in WeatherAPI forecast data: {e}")
        return None

def load_forecast_to_bigquery(data: dict, project_id: str) -> bool:
    """Loads a single transformed forecast record into BigQuery."""
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.weather_data.weather_forecasts"
        
        errors = client.insert_rows_json(table_id, [data])
        
        if errors:
            logger.error(f"BigQuery insert errors for forecast: {errors}")
            return False
        
        logger.info(f"Successfully loaded forecast for {data['city']} to BigQuery")
        return True
    except Exception as e:
        logger.error(f"BigQuery forecast loading error: {e}")
        return False

@functions_framework.http
def get_weather_forecasts(request):
    """Main ETL function for collecting and storing weather forecasts."""
    cities = [ "Aracaju", "Belém", "Belo Horizonte", "Boa Vista", "Brasília", "Campo Grande", "Cuiabá", 
              "Curitiba", "Florianópolis", "Fortaleza", "Goiânia", "João Pessoa", "Macapá", "Maceió", 
              "Manaus", "Natal", "Palmas", "Porto Alegre", "Porto Velho", "Recife", "Rio Branco", 
              "Rio de Janeiro", "Salvador", "São Luís", "São Paulo", "Teresina", "Vitória" ]
    
    api_key = os.getenv('WEATHERAPI_KEY')
    project_id = os.getenv('GCP_PROJECT')

    if not api_key or not project_id:
        msg = "WEATHERAPI_KEY or GCP_PROJECT environment variable not set."
        logger.error(msg)
        return {'error': msg}, 500

    successful_loads = 0
    for city in cities:
        raw_data = get_weatherapi_data(city, api_key)
        if raw_data:
            transformed_data = transform_weatherapi_forecast(raw_data)
            if transformed_data:
                if load_forecast_to_bigquery(transformed_data, project_id):
                    successful_loads += 1
    
    response_msg = f"ETL process completed. Successfully loaded forecasts for {successful_loads}/{len(cities)} cities."
    logger.info(response_msg)
    return {'message': response_msg}, 200