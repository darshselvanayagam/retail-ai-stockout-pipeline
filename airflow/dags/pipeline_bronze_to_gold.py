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
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            cur.execute(stmt)
        # small log so UI stays readable
        print("✅ Ran SQL block successfully", flush=True)
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
    ON_ERROR = 'CONTINUE';
    """
    run_sql(sql)


SILVER_SQL = """
CREATE SCHEMA IF NOT EXISTS RETAIL_AI.SILVER;

CREATE OR REPLACE TABLE RETAIL_AI.SILVER.SALES_EVENTS AS
SELECT
  EVENT:event_time::timestamp_ntz AS event_time,
  EVENT:event_date::date          AS event_date,
  EVENT:store_id::string          AS store_id,
  EVENT:product_id::string        AS product_id,
  EVENT:qty::number               AS qty
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:event_type::string = 'sale';

CREATE OR REPLACE TABLE RETAIL_AI.SILVER.INVENTORY_EVENTS AS
SELECT
  EVENT:event_time::timestamp_ntz AS event_time,
  EVENT:event_date::date          AS event_date,
  EVENT:store_id::string          AS store_id,
  EVENT:product_id::string        AS product_id,
  EVENT:on_hand::number           AS on_hand
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:event_type::string = 'inventory';
"""


GOLD_DIMS_SQL = """
CREATE SCHEMA IF NOT EXISTS RETAIL_AI.GOLD;

CREATE OR REPLACE TABLE RETAIL_AI.GOLD.DIM_DATE AS
SELECT DISTINCT
    EVENT:event_date::date                AS date,
    YEAR(EVENT:event_date::date)          AS year,
    MONTH(EVENT:event_date::date)         AS month,
    DAY(EVENT:event_date::date)           AS day,
    DAYOFWEEK(EVENT:event_date::date)     AS day_of_week
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:event_date IS NOT NULL;

CREATE OR REPLACE TABLE RETAIL_AI.GOLD.DIM_STORE AS
SELECT DISTINCT
    EVENT:store_id::string AS store_id
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:store_id IS NOT NULL;

CREATE OR REPLACE TABLE RETAIL_AI.GOLD.DIM_PRODUCT AS
SELECT DISTINCT
    EVENT:product_id::string AS product_id
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:product_id IS NOT NULL;
"""


GOLD_FACTS_SQL = """
CREATE OR REPLACE TABLE RETAIL_AI.GOLD.FACT_SALES_DAILY AS
SELECT
  EVENT:event_date::date      AS date,
  EVENT:store_id::string      AS store_id,
  EVENT:product_id::string    AS product_id,
  SUM(EVENT:qty::number)      AS total_qty_sold,
  COUNT(*)                    AS sales_event_count
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:event_type::string = 'sale'
GROUP BY 1,2,3;

CREATE OR REPLACE TABLE RETAIL_AI.GOLD.FACT_INVENTORY_DAILY AS
SELECT
  EVENT:event_date::date            AS date,
  EVENT:store_id::string            AS store_id,
  EVENT:product_id::string          AS product_id,
  EVENT:on_hand::number             AS on_hand_latest,
  EVENT:event_time::timestamp_ntz   AS last_inventory_time
FROM RETAIL_AI.BRONZE.RAW_EVENTS
WHERE EVENT:event_type::string = 'inventory'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY EVENT:event_date::date, EVENT:store_id::string, EVENT:product_id::string
  ORDER BY EVENT:event_time::timestamp_ntz DESC
) = 1;
"""


FEATURES_SQL = """
CREATE OR REPLACE TABLE RETAIL_AI.GOLD.FEATURES_DAILY AS
SELECT
  s.date,
  s.store_id,
  s.product_id,
  s.total_qty_sold,
  s.sales_event_count,
  i.on_hand_latest,
  CASE WHEN i.on_hand_latest <= 2 THEN 1 ELSE 0 END AS stockout_risk_label
FROM RETAIL_AI.GOLD.FACT_SALES_DAILY s
LEFT JOIN RETAIL_AI.GOLD.FACT_INVENTORY_DAILY i
  ON s.date = i.date
 AND s.store_id = i.store_id
 AND s.product_id = i.product_id;
"""


with DAG(
    dag_id="retail_pipeline_bronze_to_gold",
    start_date=datetime(2026, 2, 19),
    schedule_interval=None,  # manual while learning
    catchup=False,
    tags=["retail", "snowflake"],
) as dag:

    t1_bronze = PythonOperator(
        task_id="copy_into_bronze",
        python_callable=copy_into_bronze,
    )

    t2_silver = PythonOperator(
        task_id="build_silver",
        python_callable=lambda: run_sql(SILVER_SQL),
    )

    t3_dims = PythonOperator(
        task_id="build_gold_dims",
        python_callable=lambda: run_sql(GOLD_DIMS_SQL),
    )

    t4_facts = PythonOperator(
        task_id="build_gold_facts",
        python_callable=lambda: run_sql(GOLD_FACTS_SQL),
    )

    t5_features = PythonOperator(
        task_id="build_features",
        python_callable=lambda: run_sql(FEATURES_SQL),
    )

    t1_bronze >> t2_silver >> t3_dims >> t4_facts >> t5_features