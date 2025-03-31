import boto3
import os

# AWS S3 details
s3_client = boto3.client("s3")
bucket_name = "premier-league-football-data-bucket"
csv_filename = "2023_24_premier_league_season_data.csv"

# Ensure the "Data" directory exists
data_dir = "../Data"
os.makedirs(data_dir, exist_ok=True)

local_path = f"../Data/{csv_filename}"  # Save file in the temp directory

# Download the file from S3
s3_client.download_file(bucket_name, csv_filename, local_path)

print(f"File downloaded to {local_path}")