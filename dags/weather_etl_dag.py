from datetime import datetime
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 3),
    'retries': 1,
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
)

etl_task = DockerOperator(
    task_id='run_etl_pipeline',
    image='etl-pipeline:latest',  # Use your ETL image name
    api_version='auto',
    auto_remove=True,
    docker_url='unix://var/run/docker.sock',
    network_mode='weather_network',  # Use your Docker network
    environment={
        'API_KEY': '{{ var.value.API_KEY }}',  # Store securely in Airflow Variables
        'DB_HOST': 'postgres',
        'DB_NAME': '{{ var.value.DB_NAME }}',
        'DB_USER': '{{ var.value.DB_USER }}',
        'DB_PASS': '{{ var.value.DB_PASS }}',
    },
    dag=dag,
)

etl_task