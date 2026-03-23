# Indian Aviation Intelligence Platform

**End-to-end data engineering pipeline on GCP for Indian domestic aviation analytics**

Built on real flight data: **10,683 records** across IndiGo, Jet Airways, Air India, SpiceJet, Vistara, GoAir, Air Asia

---

## Project Objective

Raw Indian domestic flight data is messy and unqueryable — misspelled cities, mixed-format durations, next-day arrival timestamps.

**SkyLens India** builds a **production-grade, automated pipeline** that:

1. Ingests data via **Google Pub/Sub** (streaming simulation)
2. Stores partitioned NDJSON in **GCS** (data lake)
3. Orchestrates daily loads via **Apache Airflow** on Cloud Composer
4. Transforms through a **3-layer dbt model** (staging → intermediate → mart)
5. Validates with **27 automated data quality tests**

---

## Repository Structure

```bash
skylens-india/
├── data/
│   └── flight_date.xlsx            # Real dataset (10,683 rows)

├── notebooks/
│   └── eda_flight_data.ipynb       # EDA: data quality discovery

├── ingestion/
│   ├── upload_csv_to_gcs.py        # xlsx → parsed NDJSON → GCS
│   ├── pubsub_publisher.py         # Streaming simulation via Pub/Sub
│   └── pubsub_subscriber.py        # Cloud Run: Pub/Sub → GCS

├── airflow/
│   ├── requirements.txt
│   └── dags/
│       └── daily_pipeline_dag.py   # 7-task Airflow DAG

├── dbt/skylens/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_flights.sql
│       │   └── schema.yml          # 27 data quality tests
│       ├── intermediate/
│       │   └── int_flight_enriched.sql
│       └── marts/
│           ├── mart_airline_performance.sql
│           ├── mart_route_fares.sql
│           └── mart_price_analysis.sql

├── infra/
│   └── setup_gcp.sh                # One-time GCP provisioning

├── .gitignore
└── README.md
```

---

## Dataset: `flight_date.xlsx`

| Column          | Type    | Example                      | Issue Handled               |
| --------------- | ------- | ---------------------------- | --------------------------- |
| Airline         | string  | IndiGo, Jet Airways Business | Normalised variants         |
| Date_of_Journey | string  | 24/03/2019, 1/05/2019        | Inconsistent padding → DATE |
| Source          | string  | Banglore, Delhi              | Misspelling fixed           |
| Destination     | string  | Cochin, New Delhi            | Standardised                |
| Route           | string  | BLR → DEL                    | 1 null filtered             |
| Dep_Time        | string  | 22:20                        | Hour extracted              |
| Arrival_Time    | string  | 01:10 22 Mar                 | 4335 next-day rows flagged  |
| Duration        | string  | 2h 50m, 19h                  | Regex → integer minutes     |
| Total_Stops     | string  | non-stop, 1 stop             | 1 null filtered → integer   |
| Additional_Info | string  | No info                      | Casing normalised           |
| Price           | integer | 3897                         | INR range ₹1759–₹79512      |

---

## Tech Stack

| Layer          | Technology                                                   |
| -------------- | ------------------------------------------------------------ |
| Data lake      | Google Cloud Storage (3-zone: landing/archive/dbt_artifacts) |
| Streaming      | Google Pub/Sub + Cloud Run subscriber                        |
| Orchestration  | Apache Airflow on Cloud Composer                             |
| DB             | PostgreSQL on Cloud SQL (Airflow state store)                |
| Transformation | dbt Core — staging, intermediate, mart layers                |
| Warehouse      | BigQuery (date-partitioned, clustered tables)                |

---

## Airflow Pipeline

```bash
check_landing
    ↓
validate_data
    ↓
load_raw_to_bq
    ↓
dbt_run
    ↓
dbt_test
    ↓
archive
    ↓
notify

---

## Key Insights Delivered

* Non-stop flights cost **20–50% more** than 1-stop on the same route
* **Delhi → Kochi** is the most popular route (**4,537 records**)
* Jet Airways shows the **widest price variance**, including ₹79,512 outlier
* Night departures tend to be cheaper across most routes
* IndiGo dominates non-stop short-haul on **BLR → DEL corridor**

---

