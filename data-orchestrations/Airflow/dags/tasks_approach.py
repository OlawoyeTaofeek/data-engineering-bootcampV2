from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
import csv
from typing import Dict, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime
    
@dag
def task_user_processing():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"]
    }

    @task
    def process_user(user_info: Dict[str, str]):
        user_info['created_at'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)


    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename='/tmp/user_info.csv'
        )
    
    
    user_info = extract_user(create_table >> is_api_available())
    process_data = process_user(user_info)
    store_data = store_user()

    process_data >> store_data

    # create_table >> process_user(extract_user(is_api_available)) >> store_user()
            
task_user_processing()