# Real-Time Telecom Analytics Pipeline

An end-to-end data engineering project built on Databricks, processing 120,000+ daily telecom records through a medallion architecture to deliver customer churn risk and network health analytics.

![Pipeline Architecture](architecture.svg)

---

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model)
- [Project Structure](#project-structure)
- [Performance](#performance)


---

## Overview

This project simulates a production-grade telecom data pipeline that processes Call Detail Records (CDR) and network events from 400 regional towers daily. The pipeline ingests raw files from S3, applies data quality checks, aggregates customer-level metrics, and surfaces churn risk scores and network health indicators through Databricks SQL Warehouse.

**Business use case:** Identify high-risk customers before churn occurs by combining call behavior (dropped calls, missed calls, international usage) with network quality signals (tower outages, poor signal, data throttling).

---


### Medallion Architecture

| Layer | Tables | Purpose |
|---|---|---|
| Bronze | `bronze_cdr`, `bronze_network_events` | Raw ingestion via Autoloader — append only |
| Silver | `silver_cdr`, `silver_network_events` | Cleaned, validated, enriched records |
| Gold | `gold_customer_telecom` | Daily customer-level aggregations with churn metrics |

---

## Tech Stack

| Component | Technology |
|---|---|
| Data Platform | Databricks (Serverless) |
| Pipeline Framework | Lakeflow Spark Declarative Pipelines (DLT) |
| Storage Format | Delta Lake |
| Cloud Storage | AWS S3 |
| Catalog | Unity Catalog |
| Orchestration | Apache Airflow (Docker) |
| Ingestion Pattern | Autoloader + append_flow |
| Language | PySpark, Python, SQL |
| Infrastructure | AWS S3, Databricks Jobs API |

---

## Data Model

### Input Data

**CDR Files** (`raw/cdr/cdr_YYYYMMDD_NNN.csv`) — 400 files × 200 rows = 80,000 records/day
```
cdr_id, customer_id, call_date, call_time, call_duration_mins,
call_type, destination, charge, status, roaming
```

**Network Events** (`raw/network_events/events_YYYYMMDD_NNN.json`) — 400 files × 100 events = 40,000 records/day
```
event_id, customer_id, tower_id, timestamp, event_date,
event_type, signal_strength, data_speed_mbps, severity
```

**Bad data injected for realism:**
- 2% null durations
- 1% duplicate records
- 0.5% negative charges
- 0.3% future dates
- 1% invalid customer IDs


---

## Project Structure

```
telecom-pipeline/
├── dags/
│   └── telecom_daily_upload_dag.py    # Airflow DAG — upload + verify + archive
├── pipelines/
│   ├── cdr_pipeline.py                # DLT pipeline — CDR bronze + silver
│   └── events_pipeline.py             # DLT pipeline — Events bronze + silver
├── notebooks/
│   ├── gold_notebook.py               # Gold aggregation + MERGE + S3 archive
│   ├── log_cdr_completion.py          # Logs CDR completion to pipeline_run_log
│   ├── log_events_completion.py       # Logs Events completion to pipeline_run_log
│   └── check_and_trigger_gold.py      # Checks both done → triggers Gold Job API
├── data_generation/
│   └── generate_test_data.py          # Generates synthetic CDR + events data
├── architecture.svg                   # Architecture diagram
└── README.md
```


---


## Performance

| Stage | Duration |
|---|---|
| CDR pipeline (Bronze + Silver) | ~1 min 11s |
| Events pipeline (Bronze + Silver) | ~1 min 30s |
| log_cdr/events_completion | ~5s each |
| check_and_trigger_gold | ~10s |
| gold_notebook (MERGE + archive) | ~2 min |
| **Total end-to-end** | **~5 minutes** |

*Tested on Databricks Serverless compute with 80,000 CDR rows + 40,000 event rows.*
**Note:** Pipeline currently runs on Databricks Serverless compute which incurs a cold start. Actively exploring optimizations to reduce end-to-end latency. Target: under 2 minutes.

---

