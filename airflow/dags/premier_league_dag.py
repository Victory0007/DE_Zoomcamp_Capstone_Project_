from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Import the functions from your scripts
from scripts.download_data import download_data
from scripts.upload_data_to_s3_bucket import upload_and_cleanup_s3
from scripts.download_from_s3 import download_from_s3
from scripts.ingestion_to_postgres import load_data_to_postgres

# Define the default_args dictionary
default_args = {
    'owner': 'Victory',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 1),  # Adjust start date as needed
}

# Define the DAG
dag = DAG(
    'premier_league_data_pipeline',
    default_args=default_args,
    description='A DAG to scrape, upload, and store Premier League data',
    schedule_interval='@weekly',  # Adjust the schedule interval as per your needs
)

# List of seasons to process
seasons = ['2019/20', '2020/21', '2021/22', '2022/23', '2023/24']

# Loop through the seasons to create tasks dynamically
for season in seasons:
    # Replace the slash in the season with an underscore
    season_id = season.replace('/', '_')

    # Task 1: Download Data for a given season
    download_data_task = PythonOperator(
        task_id=f'download_data_{season_id}',
        python_callable=download_data,
        op_kwargs={'season': season, 'start_game_week': 1, 'end_game_week': 38},
        dag=dag,
    )

    # Task 2: Upload Data to S3
    upload_data_to_s3_task = PythonOperator(
        task_id=f'upload_data_to_s3_{season_id}',
        python_callable=upload_and_cleanup_s3,
        op_kwargs={'season': season, 'start_game_week': 1, 'end_game_week': 38, 'bucket_name': 'premier-league-football-data-bucket'},
        dag=dag,
    )

    # Task 3: Download Data from S3
    download_data_from_s3_task = PythonOperator(
        task_id=f'download_data_from_s3_{season_id}',
        python_callable=download_from_s3,
        op_kwargs={'season': season, 'start_game_week': 1, 'end_game_week': 38, 'bucket_name': 'premier-league-football-data-bucket'},
        dag=dag,
    )

    # Task 4: Load Data into PostgreSQL
    load_data_to_postgres_task = PythonOperator(
        task_id=f'load_data_to_postgres_{season_id}',
        python_callable=load_data_to_postgres,
        op_kwargs={
            'season': season,
            'start_game_week': 1,
            'end_game_week': 38,
            'db_user': 'Data_Professional',
            'db_password': 'python',
            'db_host': '127.0.0.1',
            'db_port': '5433',
            'db_name': 'premier_league_db',
            'data_dir': '../data'
        },
        dag=dag,
    )

    # Set up task dependencies for each season
    download_data_task >> upload_data_to_s3_task >> download_data_from_s3_task >> load_data_to_postgres_task
