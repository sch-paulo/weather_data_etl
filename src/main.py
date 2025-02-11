import os
import time
from pipeline import init_database, extract_city_weather_data, transform_city_weather_data, load_weather_data_on_database
from utils_log import log_decorator
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv(override=True)

# Environment variables
API_KEY = os.getenv('API_KEY')

@log_decorator
def run_pipeline(city):
    try:
        # Initialize database
        init_database()

        # Fetch weather data
        weather_data = extract_city_weather_data(city, API_KEY)

        # Fetch weather data
        weather_data = transform_city_weather_data(weather_data)

        # Store data
        load_weather_data_on_database(weather_data)

    except Exception as e:
        raise

if __name__ == "__main__":
    # List of capitals from Brazil
    brazilian_capitals = [
        "Aracaju", "Belém", "Belo Horizonte", "Boa Vista", "Brasília", "Campo Grande",
        "Cuiabá", "Curitiba", "Florianópolis", "Fortaleza", "Goiânia", "João Pessoa",
        "Macapá", "Maceió", "Manaus", "Natal", "Palmas", "Porto Alegre", "Porto Velho",
        "Recife", "Rio Branco", "Rio de Janeiro", "Salvador,BA,BR", "São Luís", "São Paulo",
        "Teresina", "Vitória"
    ]

    while True:
        for city in brazilian_capitals:
            run_pipeline(city)
        time.sleep(60)