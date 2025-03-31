import os
from scripts.crawler import Crawler
import pandas as pd

def download_data(season: str):
    """
    Downloads Premier League data for a given season and saves it as a CSV file.

    Args:
        season (str): The season to download data for (e.g., "2023/24").
    """
    # Ensure the "Data" directory exists
    data_dir = "../Data"
    os.makedirs(data_dir, exist_ok=True)

    # Format the season string for filenames
    season_clean = season.replace("/", "_")  # Convert "2023/24" -> "2023_24"

    # Initialize and run the Crawler
    crawler = Crawler(season)
    df = crawler.run()
    df["season"] = season

    # Save the DataFrame as a CSV inside the "Data" folder
    csv_filename = f"{data_dir}/{season_clean}_premier_league_season_data.csv"
    df.to_csv(csv_filename, index=False)

    print(f"Data downloaded and saved to {csv_filename}")
