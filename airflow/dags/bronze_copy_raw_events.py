import os
from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        warehouse=os.environ.get("SF_WAREHOUSE"),
        role=os.environ.get("SF_ROLE") or None,
        database="RETAIL_AI",
    )


def run_sql(sql: str):
    conn = _connect()
    try:
        cur = conn.cursor()
        # Execute statements split by semicolon (safe for simple SQL blocks)
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            cur.execute(stmt)
        print("Bronze COPY INTO completed", flush=True)
    finally:
        conn.close()


def copy_into_bronze():
    sql = r"""
    COPY INTO RETAIL_AI.BRONZE.RAW_EVENTS (EVENT)
    FROM (
      SELECT $1
      FROM @RETAIL_AI.BRONZE.S3_RAW_STAGE
    )
    FILE_FORMAT = (FORMAT_NAME = RETAIL_AI.STAGING.JSONL_FORMAT)
    PATTERN = '.*\.jsonl'
    ON_ERROR = 'CONTINUE'
    ;
    """
    run_sql(sql)


with DAG(
    dag_id="bronze_copy_raw_events",
    start_date=datetime(2026, 2, 19),
    schedule_interval=None,  # keep manual while learning
    catchup=False,
    tags=["retail", "snowflake", "bronze"],
) as dag:

    t1_copy_into_bronze = PythonOperator(
        task_id="copy_into_bronze",
        python_callable=copy_into_bronze,
    )

    t1_copy_into_bronze