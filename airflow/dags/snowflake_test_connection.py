import os
from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator


def test_snowflake():
    # These come from your docker-compose.yml env section
    account = os.environ["SF_ACCOUNT"]
    user = os.environ["SF_USER"]
    password = os.environ["SF_PASSWORD"]
    warehouse = os.environ.get("SF_WAREHOUSE")
    role = os.environ.get("SF_ROLE") or None
    database = os.environ.get("SF_DATABASE") or None
    schema = os.environ.get("SF_SCHEMA") or None

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_REGION(), CURRENT_ACCOUNT()")
        row = cur.fetchone()
        print("Snowflake connection OK:", row, flush=True)
    finally:
        conn.close()


with DAG(
    dag_id="snowflake_test_connection",
    start_date=datetime(2026, 2, 19),
    schedule_interval=None,  # manual trigger while learning
    catchup=False,
    tags=["snowflake"],
) as dag:
    PythonOperator(
        task_id="test_connection",
        python_callable=test_snowflake,
    )