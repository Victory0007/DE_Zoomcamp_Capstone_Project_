import os
import sys
sys.path.append('/opt/airflow/dags/scripts')

from crawler import Crawler


def download_data(season: str, start_game_week=1, end_game_week=3):
    """Downloads Premier League data for specific game weeks and saves it to CSV."""
    data_dir = '../data'

    os.makedirs(data_dir, exist_ok=True)

    season_clean = season.replace("/", "_")

    crawler = Crawler(season)
    df = crawler.run(start_game_week, end_game_week)
    df["season"] = season

    for game_week in range(start_game_week, end_game_week + 1):
        game_week_df = df[df["game_week"] == game_week]
        csv_filename = f"{data_dir}/{season_clean}_GW{game_week}_data.csv"
        game_week_df.to_csv(csv_filename, index=False)

        print(f"Game Week {game_week} data saved: {csv_filename}")

if __name__ == "__main__":
    SEASONS = ["2019/20"]
    for season in SEASONS:
        download_data(season)

