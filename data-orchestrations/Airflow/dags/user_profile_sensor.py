from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -----------------------
# Python callables
# -----------------------
def process_user(ti):
    """Pull user JSON from fetch_user and write CSV"""
    user = ti.xcom_pull(task_ids="fetch_user")
    user['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not user:
        raise ValueError("No user data to process")

    with open("/tmp/user_info.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "firstname", "lastname", "email", "created_at"])
        writer.writeheader()
        writer.writerow({
            "id": user["id"],
            "firstname": user["personalInfo"]["firstName"],
            "lastname": user["personalInfo"]["lastName"],
            "email": user["personalInfo"]["email"],
            "created_at": user['created_at']
        })


# -----------------------
# DAG Definition
# -----------------------
@dag
def custom_sensor_user_profile():
    # Task 1: Create table
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

    # Task 2: Sensor to wait for API
    wait_for_api = HttpSensor(
        task_id="wait_for_api",
        http_conn_id="github_api",
        endpoint="marclamberti/datasets/refs/heads/main/fakeuser.json",
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=600,
    )

    # Task 3: Fetch user JSON
    fetch_user = HttpOperator(
        task_id="fetch_user",
        http_conn_id="github_api",
        endpoint="marclamberti/datasets/refs/heads/main/fakeuser.json",
        method="GET",
        response_filter=lambda response: response.json(),
        log_response=True
    )

    # Task 4: Process user (write CSV, could also insert into Postgres)
    process_user_task = PythonOperator(
        task_id="process_user",
        python_callable=process_user
    )

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename='/tmp/user_info.csv'
        )

    store_data = store_user()
    # -----------------------
    # Task Dependencies
    # -----------------------
    create_table >> wait_for_api >> fetch_user >> process_user_task >> store_data


# Instantiate DAG
custom_sensor_user_profile()