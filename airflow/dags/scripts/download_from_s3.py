import boto3
import os

def download_from_s3(season: str, start_game_week=1, end_game_week=38, bucket_name="premier-league-football-data-bucket"):
    """Downloads specific game weeks from S3."""
    s3_client = boto3.client("s3")
    data_dir = '../data'
    os.makedirs(data_dir, exist_ok=True)

    season_clean = season.replace("/", "_")

    for game_week in range(start_game_week, end_game_week + 1):
        csv_filename = f"{season_clean}_GW{game_week}_data.csv"
        local_path = os.path.join(data_dir, csv_filename)

        try:
            s3_client.download_file(bucket_name, csv_filename, local_path)
            print(f"Downloaded {csv_filename} to {local_path}")
        except Exception as e:
            print(f"Error downloading {csv_filename}: {e}")


if __name__ == "__main__":
    download_from_s3("2019/20", start_game_week=1, end_game_week=3)

