import os
import time
import schedule
import multiprocessing
from datetime import datetime

# Import necessary scripts
from scripts import download_data
from scripts.upload_data_to_s3_bucket import upload_and_cleanup_s3
from scripts import download_from_s3
from scripts.ingestion_to_postgres import load_data_to_postgres

# Define the seasons for which data will be fetched
SEASONS = ["2019/20", "2020/21", "2021/22", "2022/23", "2023/24"]
START_GAME_WEEK = 1
END_GAME_WEEK = 38

def process_season(season):
    """Pipeline to process a season."""
    print(f"Starting pipeline for {season} at {datetime.now()}")

    try:
        # Step 1: Download data
        print(f"Downloading data for {season}...")
        download_data(season, START_GAME_WEEK, END_GAME_WEEK)

        # Step 2: Upload to S3
        print(f"Uploading data for {season} to S3...")
        upload_and_cleanup_s3(season)

        # Step 3: Download from S3
        print(f"Downloading data for {season} from S3...")
        download_from_s3(season)

        # Step 4: Load data into PostgreSQL
        print(f"Loading data for {season} into PostgreSQL...")
        load_data_to_postgres(season)

        print(f"Completed pipeline for {season} ✅\n")

    except Exception as e:
        print(f"Error processing {season}: {e}")

def run_pipeline():
    """Runs the pipeline for all seasons in parallel."""
    print(f"\n--- Running pipeline at {datetime.now()} ---")

    processes = []
    for season in SEASONS:
        p = multiprocessing.Process(target=process_season, args=(season,))
        p.start()
        processes.append(p)

    # Wait for all processes to complete
    for p in processes:
        p.join()

def run_scheduler():
    """Runs the pipeline immediately, then schedules it every Sunday at midnight."""
    print("🚀 Starting initial data processing for all seasons...")
    run_pipeline()  # Run immediately

    print("📅 Scheduling weekly updates every Sunday at midnight...")
    schedule.every().sunday.at("00:00").do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    run_scheduler()
