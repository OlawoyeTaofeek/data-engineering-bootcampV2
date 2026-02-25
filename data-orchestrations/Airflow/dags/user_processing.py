from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
import requests
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime

# -----------------------
# Python callables
# -----------------------
def _extract_user(ti):
    # Pull XCom from sensor task
    fake_user = ti.xcom_pull(task_ids="is_api_available")
    if not fake_user:
        raise ValueError("API did not return user data")

    return {
        "id": fake_user['id'],
        "firstname": fake_user['personalInfo']['firstName'],
        "lastname": fake_user['personalInfo']['lastName'],  # fixed typo
        "email": fake_user['personalInfo']['email'],
    }

def _process_user(ti):
    # Pull XCom from extract_user task
    user = ti.xcom_pull(task_ids="extract_user")
    if not user:
        raise ValueError("No user data to process")
    user['created_at'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("/tmp/user_info.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=user.keys())
        writer.writeheader()
        writer.writerow(user)


def _store_user():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY users FROM STDIN WITH CSV HEADER",
        filename="/tmp/user_info.csv"
    )

# -----------------------
# DAG Definition
# -----------------------
@dag
def user_processing():

    # TASK 1: Create table
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

    # TASK 2: Sensor to wait for API
    @task.sensor(poke_interval=30, timeout=600)
    def is_api_available() -> PokeReturnValue:
        response = requests.get(
            "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
        )
        if response.status_code == 200:
            fake_user = response.json()
            return PokeReturnValue(is_done=True, xcom_value=fake_user)
        else:
            return PokeReturnValue(is_done=False, xcom_value=None)

    api_available = is_api_available()  # assign to variable for DAG

    # TASK 3: Extract user
    extract_user = PythonOperator(
        task_id="extract_user",
        python_callable=_extract_user,
    )

    # TASK 4: Process user
    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user,
    )

    store_user_task = PythonOperator(
        task_id="store_user",
        python_callable=_store_user
    )
    # -----------------------
    # Task Dependencies
    # -----------------------
    create_table >> api_available >> extract_user >> process_user >> store_user_task

# Instantiate DAG
user_processing()