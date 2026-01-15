import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from dataclasses import dataclass

def build_taxi_url(year: int, month: int) -> str:
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    return (
        f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    )

# -----------------------------
# Config Class
# -----------------------------
@dataclass(frozen=True)
class ConnectionEngine:
    data_url: str
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
        print("Loading data in chunks...")

        return pd.read_csv(
            self.config.data_url,
            dtype=self.dtype,
            parse_dates=self.parse_dates,
            chunksize=self.config.chunk_size
        )

    def create_table(self, df, table_name: str):
        print(f"Creating table: {table_name}")

        df.head(0).to_sql(
            name=table_name,
            con=self.engine,
            if_exists="replace",
            index=False
        )

    def insert_chunks(self, df_iterator, table_name: str):
        print("Inserting data in chunks...")

        for i, chunk in enumerate(tqdm(df_iterator, desc="Uploading chunks"), start=1):
            chunk.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            print("Inserted Chunk:", len(chunk))

        print("All chunks inserted successfully.")

    def run(self, table_name: str):
        df_iterator = self.load_data_in_chunks()

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


# -----------------------------
# Main Script Entry Point
# -----------------------------
def main(year, month):

    data_url = build_taxi_url(year, month)
    config = ConnectionEngine(
        data_url=data_url,
        connection_url="postgresql+psycopg2://root:root@localhost:5432/ny_taxi", # 'postgresql://root:root@localhost:5432/ny_taxi'
        chunk_size=100_000
    )

    from parameters import parse_dates, dtype

    pipeline = DataIngestion(
        config=config,
        dtype=dtype,
        parse_dates=parse_dates
    )

    pipeline.run("yellow_taxi_data")


# -----------------------------
# Script Runner
# -----------------------------
if __name__ == "__main__":
    main(2021, 1)
