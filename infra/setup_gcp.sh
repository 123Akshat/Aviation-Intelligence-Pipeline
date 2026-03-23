#!/bin/bash
# ============================================================
# SkyLens India — GCP Infrastructure Setup
# Provisions all resources needed for the flight analytics pipeline
# Usage: bash setup_gcp.sh
# ============================================================

set -e

PROJECT_ID="skylens-india"
REGION="asia-south1"        # Mumbai — closest to Indian data
ZONE="asia-south1-a"

# Resource names
GCS_BUCKET="skylens-datalake-${PROJECT_ID}"
PUBSUB_TOPIC="flights-topic"
PUBSUB_SUBSCRIPTION="flights-sub"
BQ_DATASET_RAW="skylens_raw"
BQ_DATASET_DBT="skylens_dbt"
COMPOSER_ENV="skylens-composer"
POSTGRES_INSTANCE="skylens-pg"
SERVICE_ACCOUNT="skylens-sa"

echo "========================================"
echo "  SkyLens India — GCP Setup"
echo "  Project : $PROJECT_ID | Region: $REGION"
echo "========================================"

# ── 1. Set project ───────────────────────────────────────────
echo -e "\n[1/10] Setting project..."
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION

# ── 2. Enable APIs ───────────────────────────────────────────
echo -e "\n[2/10] Enabling APIs..."
gcloud services enable \
  pubsub.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  composer.googleapis.com \
  sqladmin.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com

# ── 3. Service Account ───────────────────────────────────────
echo -e "\n[3/10] Creating service account..."
gcloud iam service-accounts create $SERVICE_ACCOUNT \
  --display-name="SkyLens Pipeline SA"

SA_EMAIL="${SERVICE_ACCOUNT}@${PROJECT_ID}.iam.gserviceaccount.com"

# Roles needed across the pipeline
for ROLE in \
  "roles/bigquery.dataEditor" \
  "roles/bigquery.jobUser" \
  "roles/storage.objectAdmin" \
  "roles/pubsub.editor" \
  "roles/cloudsql.client" \
  "roles/logging.logWriter"; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" --quiet
done

# Key for local dev — NEVER commit this file
gcloud iam service-accounts keys create ./service_account.json \
  --iam-account=$SA_EMAIL
echo "Key saved to ./service_account.json — this is in .gitignore"

# ── 4. GCS Data Lake ─────────────────────────────────────────
echo -e "\n[4/10] Creating GCS data lake..."
gsutil mb -p $PROJECT_ID -c STANDARD -l $REGION gs://${GCS_BUCKET}/

# 3-zone folder structure (placeholder .keep files create logical folders)
# landing/ ← raw data arrives here from Pub/Sub subscriber / CSV upload
# archive/ ← data moved here after successful BQ load
# dbt/     ← dbt artifacts
for FOLDER in landing archive dbt_artifacts; do
  echo "placeholder" | gsutil cp - gs://${GCS_BUCKET}/${FOLDER}/.keep
done

# Lifecycle policy:
# landing/ files → Nearline storage after 7 days (cheaper)
# archive/ files → delete after 90 days (clean up old partitions)
cat > /tmp/gcs_lifecycle.json << 'LIFECYCLE'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 7, "matchesPrefix": ["landing/"]}
    },
    {
      "action": {"type": "Delete"},
      "condition": {"age": 90, "matchesPrefix": ["archive/"]}
    }
  ]
}
LIFECYCLE
gsutil lifecycle set /tmp/gcs_lifecycle.json gs://${GCS_BUCKET}/
echo "Bucket: gs://${GCS_BUCKET}/ — lifecycle applied"

# ── 5. Pub/Sub ───────────────────────────────────────────────
echo -e "\n[5/10] Creating Pub/Sub topic + subscription..."
gcloud pubsub topics create $PUBSUB_TOPIC \
  --message-retention-duration=7d

gcloud pubsub subscriptions create $PUBSUB_SUBSCRIPTION \
  --topic=$PUBSUB_TOPIC \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  --expiration-period=never

echo "Topic: $PUBSUB_TOPIC | Subscription: $PUBSUB_SUBSCRIPTION"

# ── 6. BigQuery ──────────────────────────────────────────────
echo -e "\n[6/10] Creating BigQuery datasets and raw table..."

bq mk --dataset --location=$REGION \
  --description="SkyLens raw ingested flight data" \
  ${PROJECT_ID}:${BQ_DATASET_RAW}

bq mk --dataset --location=$REGION \
  --description="SkyLens dbt-transformed models" \
  ${PROJECT_ID}:${BQ_DATASET_DBT}

# Raw flights table — schema matches the real flight_date.xlsx columns
# Partitioned by journey_date for efficient daily queries
bq mk --table \
  --description="Raw Indian domestic flight records from CSV/Pub/Sub" \
  --time_partitioning_field=journey_date \
  --time_partitioning_type=DAY \
  ${PROJECT_ID}:${BQ_DATASET_RAW}.raw_flights \
  airline:STRING,\
date_of_journey:STRING,\
source:STRING,\
destination:STRING,\
route:STRING,\
dep_time:STRING,\
arrival_time:STRING,\
duration:STRING,\
total_stops:STRING,\
additional_info:STRING,\
price:INTEGER,\
journey_date:DATE,\
ingested_at:TIMESTAMP

echo "BigQuery: $BQ_DATASET_RAW.raw_flights created (partitioned by journey_date)"

# ── 7. PostgreSQL for Airflow metadata ───────────────────────
echo -e "\n[7/10] Creating PostgreSQL (Airflow metadata DB)..."
# NOTE: Cloud Composer 2 manages its own internal database.
# This PostgreSQL is used if you want an EXTERNAL metadata DB
# or to run Airflow locally pointing at Cloud SQL.
gcloud sql instances create $POSTGRES_INSTANCE \
  --database-version=POSTGRES_14 \
  --tier=db-f1-micro \
  --region=$REGION \
  --storage-type=SSD \
  --storage-size=10GB \
  --no-assign-ip \
  --quiet

gcloud sql databases create airflow_metadata --instance=$POSTGRES_INSTANCE

POSTGRES_PASSWORD="skylens_pg_2024"   # Change in production!
gcloud sql users create airflow_user \
  --instance=$POSTGRES_INSTANCE \
  --password=$POSTGRES_PASSWORD

PG_CONN=$(gcloud sql instances describe $POSTGRES_INSTANCE --format='value(connectionName)')
echo "PostgreSQL: $POSTGRES_INSTANCE"
echo "Connection: $PG_CONN"
echo "DB: airflow_metadata | User: airflow_user"
echo ""
echo "Airflow connection string (set as AIRFLOW__DATABASE__SQL_ALCHEMY_CONN):"
echo "postgresql+psycopg2://airflow_user:${POSTGRES_PASSWORD}@/airflow_metadata?host=/cloudsql/${PG_CONN}"

# ── 8. Cloud Composer 2 ──────────────────────────────────────
echo -e "\n[8/10] Creating Cloud Composer 2 environment (~20 mins)..."
gcloud composer environments create $COMPOSER_ENV \
  --location=$REGION \
  --image-version=composer-2.5.0-airflow-2.6.3 \
  --environment-size=small \
  --service-account=$SA_EMAIL \
  --env-variables=\
GCS_BUCKET=${GCS_BUCKET},\
BQ_PROJECT=${PROJECT_ID},\
BQ_DATASET_RAW=${BQ_DATASET_RAW},\
BQ_DATASET_DBT=${BQ_DATASET_DBT},\
PUBSUB_TOPIC=${PUBSUB_TOPIC},\
SLACK_WEBHOOK_URL=REPLACE_WITH_YOUR_SLACK_WEBHOOK

AIRFLOW_UI=$(gcloud composer environments describe $COMPOSER_ENV \
  --location=$REGION --format='value(config.airflowUri)')
COMPOSER_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
  --location=$REGION --format='value(config.dagGcsPrefix)')

echo "Airflow UI: $AIRFLOW_UI"
echo "DAG bucket: $COMPOSER_BUCKET"

# ── 9. Upload DAG + install packages ─────────────────────────
echo -e "\n[9/10] Uploading DAG and installing Python packages..."
gsutil cp ../airflow/dags/daily_pipeline_dag.py ${COMPOSER_BUCKET}/

gcloud composer environments update $COMPOSER_ENV \
  --location=$REGION \
  --update-pypi-packages-from-file=../airflow/requirements.txt

# ── 10. dbt setup ────────────────────────────────────────────
echo -e "\n[10/10] Installing dbt..."
pip install dbt-bigquery==1.7.0 --break-system-packages -q
cd ../dbt/skylens && dbt deps
echo "dbt ready."

# ── Summary ─────────────────────────────────────────────────
echo ""
echo "========================================"
echo "  SETUP COMPLETE"
echo "========================================"
echo "  GCS:        gs://${GCS_BUCKET}/"
echo "  Pub/Sub:    $PUBSUB_TOPIC"
echo "  BigQuery:   $BQ_DATASET_RAW + $BQ_DATASET_DBT"
echo "  PostgreSQL: $POSTGRES_INSTANCE"
echo "  Airflow UI: $AIRFLOW_UI"
echo ""
echo "Next steps:"
echo "  1. Update SLACK_WEBHOOK_URL in Composer env vars"
echo "  2. python ingestion/upload_csv_to_gcs.py"
echo "  3. Trigger DAG in Airflow UI or wait for scheduled run"
echo "  4. cd dbt/skylens && dbt run && dbt test"
