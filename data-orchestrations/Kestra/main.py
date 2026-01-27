import pandas as pd
from sqlalchemy import create_engine

def main(filepath, user, password, host, port, db):
    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(connection_url)
    table_name = "zones"
    try:
        df = pd.read_csv(filepath)

        if df.empty:
            print("Empty DataFrame returned")
            return

        print(f"Creating table: {table_name}")

        df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False
        )

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )

        print("Data ingestion completed successfully.")

    except FileNotFoundError as e:
        print(f"Error: {e}")

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main("data/taxi_zone_lookup.csv", "root", "root", "localhost", 5432, "ny_taxi")
