from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://root:root@localhost:5433/ny_taxi")

try:
    conn = engine.connect()
    print("Connected successfully!")
    conn.close()
except Exception as e:
    print("Connection failed:", e)
