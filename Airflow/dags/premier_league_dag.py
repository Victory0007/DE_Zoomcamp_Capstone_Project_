from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Add project path to system path
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scripts import download_data
from scripts.upload_data_to_s3_bucket import upload_and_cleanup_s3
from scripts import download_from_s3
from scripts.ingestion_to_postgres import load_data_to_postgres

# Define the seasons for which data will be fetched
SEASONS = ["2019/20", "2020/21", "2021/22", "2022/23", "2023/24"]

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),  # Adjust start date as needed
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "premier_league_ingestion",
    default_args=default_args,
    description="Automates weekly ingestion of Premier League data for multiple seasons",
    schedule_interval="0 0 * * 0",  # Runs every Sunday at midnight
    catchup=False,
) as dag:

    for season in SEASONS:
        # Task 1: Download data
        download_task = PythonOperator(
            task_id=f"download_data_{season.replace('/', '_')}",
            python_callable=download_data,
            op_args=[season],
        )

        # Task 2: Upload data to S3
        upload_task = PythonOperator(
            task_id=f"upload_to_s3_{season.replace('/', '_')}",
            python_callable=upload_and_cleanup_s3,
            op_args=[season],
        )

        # Task 3: Download data from S3
        download_s3_task = PythonOperator(
            task_id=f"download_from_s3_{season.replace('/', '_')}",
            python_callable=download_from_s3,
            op_args=[season],
        )

        # Task 4: Loaddata into PostgreSQL
        load_postgres_task = PythonOperator(
            task_id=f"load_to_postgres_{season.replace('/', '_')}",
            python_callable=load_data_to_postgres,
            op_args=[season],
        )

        # Define task dependencies
        download_task >> upload_task >> download_s3_task >> load_postgres_task
