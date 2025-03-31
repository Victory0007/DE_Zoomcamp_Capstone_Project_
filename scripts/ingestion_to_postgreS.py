import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL credentials
DB_USER = "Data_Professional"
DB_PASSWORD = "python"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "premier_league_db"
TABLE_NAME = "premier_league_data"

# Create database connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

csv_filename = "2023_24_premier_league_season_data.csv"

# Read CSV into DataFrame
df = pd.read_csv(f"/home/Data_Professional/PycharmProjects/DE_Zoomcamp_Capstone_Project/Data/{csv_filename}")

# Load data into PostgreSQL
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

print(f"Data successfully loaded into PostgreSQL table: {TABLE_NAME}")
