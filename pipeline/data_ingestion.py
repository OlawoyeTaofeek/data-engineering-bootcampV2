import pandas as pd
from sqlalchemy import create_engine
from dataclasses import dataclass
from tqdm import tqdm
from typing import Optional

# -----------------------------
# Config Class
# -----------------------------
@dataclass(frozen=True)
class ConnectionEngine:
    data_url: Optional[str]
    connection_url: str
    chunk_size: int = 100_000


# -----------------------------
# Data Ingestion Class
# -----------------------------
class DataIngestion:

    def __init__(self, config: ConnectionEngine, dtype=None, parse_dates=None):
        self.config = config
        self.engine = self._create_engine()
        self.dtype = dtype
        self.parse_dates = parse_dates

    def _create_engine(self):
        return create_engine(self.config.connection_url)

    def load_data_in_chunks(self):
        """Returns an iterator of DataFrame chunks"""
        print(f"Loading data from {self.config.data_url} in chunks of {self.config.chunk_size}...")
        return pd.read_csv(
            self.config.data_url,
            dtype=self.dtype,
            parse_dates=self.parse_dates,
            chunksize=self.config.chunk_size
        )

    def create_table(self, df, table_name: str):
        """Creates an empty table in Postgres based on the first chunk"""
        print(f"Creating table: {table_name}")
        df.head(0).to_sql(
            name=table_name,
            con=self.engine,
            if_exists="replace",
            index=False
        )

    def insert_chunks(self, df_iterator, table_name: str):
        """Inserts DataFrame chunks into Postgres with tqdm progress bar"""
        for i, chunk in enumerate(tqdm(df_iterator, desc="Uploading chunks"), start=1):
            chunk.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            print(f"Inserted chunk {i}, {len(chunk)} rows")

        print("All chunks inserted successfully.")

    def run(self, table_name: str):
        df_iterator = self.load_data_in_chunks()

        # Get first chunk for schema creation
        first_chunk = next(df_iterator)
        self.create_table(first_chunk, table_name)

        # Insert first chunk
        first_chunk.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        # Insert remaining chunks
        self.insert_chunks(df_iterator, table_name)

def build_taxi_url(year: int, month: int) -> str:
    """Construct the NYC TLC Yellow Taxi CSV URL from GitHub"""
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    return (
        f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    )


import click
@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of taxi data')
@click.option('--month', default=11, type=int, help='Month of taxi data')
def ingest_data(user, password, host, port, db, year, month):
    """Command-line ETL for NYC Yellow Taxi Data"""

    # Build CSV URL dynamically
    data_url = build_taxi_url(year, month)

    # Build PostgreSQL connection string
    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    config = ConnectionEngine(
        data_url=data_url,
        connection_url=connection_url,
        chunk_size=100_000
    )
    from parameters import parse_dates, dtype

    pipeline = DataIngestion(config=config, dtype=dtype, parse_dates=parse_dates)

    # Dynamic table name based on year/month
    # table_name = f"yellow_taxi_data_{year}_{month:02d}"
    table_name = "yellow_taxi_data_2025"

    pipeline.run(table_name)



if __name__ == "__main__":
    ingest_data()

