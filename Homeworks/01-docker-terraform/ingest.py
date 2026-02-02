import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
from tqdm import tqdm


def ingest_file(
    filepath: str,
    user: str,
    password: str,
    host: str,
    port: int,
    db: str,
    table_name: str,
    chunk_size: Optional[int] = 100_000
):
    """
    Ingest CSV or Parquet file into PostgreSQL with progress tracking.
    """
    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(connection_url)

    try:
        # -------------------------
        # CSV FILE (CHUNKED)
        # -------------------------
        if filepath.endswith(".csv"):
            print(f"Reading CSV file in chunks: {filepath}")

            iterator = pd.read_csv(filepath, chunksize=chunk_size)
            first_chunk = next(iterator)

            print(f"Creating table: {table_name}")
            first_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )

            first_chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False
            )

            print(f"Inserted first chunk ({len(first_chunk)} rows)")

            for chunk in tqdm(iterator, desc="Uploading CSV chunks"):
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists="append",
                    index=False
                )

        # -------------------------
        # PARQUET FILE (MANUAL CHUNKING)
        # -------------------------
        elif filepath.endswith(".parquet"):
            print(f"Reading Parquet file: {filepath}")
            df = pd.read_parquet(filepath)

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

            total_rows = len(df)
            print(f"Total rows: {total_rows}")

            for start in tqdm(range(0, total_rows, chunk_size), desc="Uploading Parquet chunks"):
                end = start + chunk_size
                chunk = df.iloc[start:end]

                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists="append",
                    index=False
                )

        else:
            raise ValueError("File must be .csv or .parquet")

        print("✅ Data ingestion completed successfully.")

    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    ingest_file(
        filepath="taxi_zone_lookup.csv",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5434,
        db="ny_taxi",
        table_name="zones"
    )
