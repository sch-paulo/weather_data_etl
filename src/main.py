import os
import time
from pipeline import init_bigquery_table, extract_city_weather_data, transform_city_weather_data, load_weather_data_to_bigquery
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
        init_bigquery_table()

        # Fetch weather data
        weather_data = extract_city_weather_data(city, API_KEY)

        # Fetch weather data
        if weather_data:
            transformed_data = transform_city_weather_data(weather_data)

            # Store data
            if transformed_data:
                success = load_weather_data_to_bigquery(transformed_data)
                if success:
                    print(f'Successfully processed {city}!')
                else:
                    print(f'Failed to load {city}')
            else:
                print(f'Failed to transform data for {city}')
        else:
            print(f'Failed to extract data for {city}')

    except Exception as e:
        print(f'Pipeline failed for {city}: {e}')
        raise

if __name__ == "__main__":
    # List of capitals from Brazil
    # brazilian_capitals = [
    #     "Aracaju", "Belém", "Belo Horizonte", "Boa Vista", "Brasília", "Campo Grande",
    #     "Cuiabá", "Curitiba", "Florianópolis", "Fortaleza", "Goiânia", "João Pessoa",
    #     "Macapá", "Maceió", "Manaus", "Natal", "Palmas", "Porto Alegre", "Porto Velho",
    #     "Recife", "Rio Branco", "Rio de Janeiro", "Salvador,BA,BR", "São Luís", "São Paulo",
    #     "Teresina", "Vitória"
    # ]

    test_cities = ["São Paulo", "Rio de Janeiro", "Brasília"]
    for city in test_cities:
        run_pipeline(city)
        time.sleep(2)

    print("Test complete! Check BigQuery console.")

    