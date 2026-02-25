# Retail AI Data Platform

End-to-end real-time retail data pipeline with ML-powered stockout prediction.

---

## Architecture Overview

Kafka Producer
      ↓
Kafka Broker
      ↓
Kafka Consumer
      ↓
Amazon S3 (Raw JSONL files)
      ↓
Snowflake Bronze Layer (VARIANT raw ingestion)
      ↓
Silver Layer (Structured tables)
      ↓
Gold Layer (Dimensional model + aggregates)
      ↓
Feature Engineering
      ↓
Logistic Regression Model
      ↓
Prediction Table (STOCKOUT_PREDICTIONS)

---

## Data Flow

### 1. Streaming Ingestion

- Producer generates simulated retail events (sales + inventory)
- Consumer writes batched JSONL files to S3
- S3 lifecycle policies control storage cost

### 2. Bronze Layer

Raw JSON is loaded into Snowflake:

RETAIL_AI.BRONZE.RAW_EVENTS

Stored as VARIANT for schema flexibility.

### 3. Silver Layer

Structured tables created from VARIANT:

- SALES_EVENTS
- INVENTORY_EVENTS

### 4. Gold Layer

Dimensional modeling:

- DIM_DATE
- DIM_STORE
- DIM_PRODUCT
- FACT_SALES_DAILY
- FACT_INVENTORY_DAILY

### 5. Feature Engineering

FEATURES_DAILY combines:

- Total quantity sold
- Sales event count
- Latest inventory level
- Rule-based stockout label

### 6. Machine Learning Layer

A Logistic Regression model is trained inside Airflow using:

- TOTAL_QTY_SOLD
- SALES_EVENT_COUNT
- ON_HAND_LATEST

Predictions are written to:

RETAIL_AI.GOLD.STOCKOUT_PREDICTIONS

Each pipeline run:

- Rebuilds transformed tables
- Retrains model
- Rewrites prediction table

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Streaming | Apache Kafka |
| Storage | Amazon S3 |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow |
| ML | scikit-learn |
| Containerization | Docker |
| Language | Python |

---

## How to Run Locally

### Start Kafka + Producer + Consumer

```bash
docker compose up -d
```

### Start Airflow

Inside airflow/ directory:

```bash
docker compose up -d --build
```

Access Airflow UI:

http://localhost:8080

Trigger DAG:

pipeline_bronze_to_gold

---

## Design Decisions

- Used layered architecture (Bronze → Silver → Gold) for separation of concerns
- Implemented full-refresh pipeline for simplicity and idempotency
- Integrated ML training directly in Airflow for orchestration consistency
- Used VARIANT storage in Bronze for schema flexibility
- Structured dimensional model in Gold for analytical querying

---

## Example Queries

```sql
SELECT *
FROM RETAIL_AI.GOLD.FACT_SALES_DAILY
ORDER BY DATE DESC
LIMIT 10;
```

```sql
SELECT *
FROM RETAIL_AI.GOLD.STOCKOUT_PREDICTIONS
ORDER BY PRED_STOCKOUT_PROB DESC
LIMIT 10;
```

---

## Future Improvements

- Incremental loading instead of full refresh
- Model versioning & experiment tracking
- Data quality validation layer
- CI/CD integration
- Terraform-based infrastructure provisioning

---

## What This Project Demonstrates

- Real-time streaming ingestion
- Warehouse data modeling
- Batch orchestration with Airflow
- End-to-end ML integration
- Cost-aware architecture design
- Production-style repository structure