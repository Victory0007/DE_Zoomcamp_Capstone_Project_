import os
import boto3
import shutil

# Initialize S3 client
s3 = boto3.client("s3")
bucket_name = "premier-league-football-data-bucket"

# Define the season
season = "2023/24"
season_clean = season.replace("/", "_")  # Convert "2023/24" -> "2023_24"

# Define the file path
data_dir = "../Data"
csv_filename = f"{data_dir}/{season_clean}_premier_league_season_data.csv"

# Upload to S3
s3.upload_file(csv_filename, bucket_name, f"{season_clean}_premier_league_season_data.csv")
print(f"File uploaded to s3://{bucket_name}/{season_clean}_premier_league_season_data.csv")

# Delete the file after uploading
if os.path.exists(csv_filename):
    os.remove(csv_filename)
    print(f"Deleted file {csv_filename}")

# Delete the Data directory if it's empty
if os.path.exists(data_dir) and not os.listdir(data_dir):
    shutil.rmtree(data_dir)
    print(f"Deleted the empty directory {data_dir}")
