import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, inspect
from sqlalchemy.orm import sessionmaker
import os

def load_data_to_postgres(
    season: str,
    start_game_week=1,
    end_game_week=38,
    db_user="Data_Professional",
    db_password="python",
    db_host="127.0.0.1",
    db_port="5433",
    db_name="premier_league_db",
    data_dir="../data"
):
    """Loads each game week's data incrementally into PostgreSQL using SQLAlchemy's connection."""
    # Create the engine
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    table_name = f"{season}_premier_league_data"

    season_clean = season.replace("/", "_")

    for game_week in range(start_game_week, end_game_week + 1):
        csv_filename = f"{data_dir}/{season_clean}_GW{game_week}_data.csv"

        if not os.path.exists(csv_filename):
            print(f"File {csv_filename} not found, skipping...")
            continue

        # Read the CSV data into a pandas DataFrame
        df = pd.read_csv(csv_filename)

        # Use SQLAlchemy connection
        with engine.connect() as conn:
            # Initialize SQLAlchemy metadata
            metadata = MetaData(bind=engine)

            # Use SQLAlchemy inspect to check if the table exists in PostgreSQL
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                # If the table does not exist, create it
                df.head(0).to_sql(table_name, con=engine, if_exists='replace', index=False)
                print(f"Table {table_name} created and data uploaded.")
            else:
                # If the table exists, check for duplicate rows before appending
                table = Table(table_name, metadata, autoload_with=engine)

                # Create a session
                Session = sessionmaker(bind=engine)
                session = Session()

                try:
                    # Create a list of dictionaries from the dataframe rows
                    records = df.to_dict(orient='records')

                    for record in records:
                        # Create a filter condition based on the primary key or unique field(s)
                        # Assuming you have a unique key or primary key column
                        filter_conditions = [
                            getattr(table.c, col) == record[col] for col in df.columns
                        ]

                        # Check if the row already exists
                        existing_row = session.query(table).filter(*filter_conditions).first()

                        if existing_row is None:  # If the row doesn't exist
                            # Append the new record to the table
                            session.execute(table.insert().values(record))
                            print(f"Row inserted into {table_name}.")
                        else:
                            print(f"Row already exists in {table_name}. Skipping insertion.")

                    # Commit the session
                    session.commit()

                except Exception as e:
                    # Handle exceptions (e.g., unique constraint violations)
                    session.rollback()
                    print(f"Error occurred: {e}")

                finally:
                    # Close the session
                    session.close()

        # Remove the file after loading
        os.remove(csv_filename)

if __name__ == "__main__":
    load_data_to_postgres("2019/20", start_game_week=1, end_game_week=3)
