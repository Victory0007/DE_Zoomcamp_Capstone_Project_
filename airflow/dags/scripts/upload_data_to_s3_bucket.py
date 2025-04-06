import os
import boto3
import shutil

def upload_and_cleanup_s3(season: str, start_game_week=1, end_game_week=38, bucket_name="premier-league-football-data-bucket"):
    """Uploads each game week's data to S3 and cleans up locally."""
    s3 = boto3.client("s3")
    data_dir = '../data'
    season_clean = season.replace("/", "_")

    for game_week in range(start_game_week, end_game_week + 1):
        csv_filename = f"{data_dir}/{season_clean}_GW{game_week}_data.csv"

        try:
            s3.upload_file(csv_filename, bucket_name, f"{season_clean}_GW{game_week}_data.csv")
            print(f"Uploaded to s3://{bucket_name}/{season_clean}_GW{game_week}_data.csv")

            os.remove(csv_filename)
            print(f"Deleted local file: {csv_filename}")

        except Exception as e:
            print(f"Error uploading {csv_filename}: {e}")

    if os.path.exists(data_dir) and not os.listdir(data_dir):
        shutil.rmtree(data_dir)
        print(f"Deleted empty directory {data_dir}")

if __name__ == "__main__":
    upload_and_cleanup_s3("2019/20", start_game_week=1, end_game_week=3)

