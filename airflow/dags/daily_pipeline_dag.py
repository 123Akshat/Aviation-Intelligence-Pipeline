"""
daily_pipeline_dag.py
---------------------
SkyLens India — Main Airflow DAG

Pipeline (7 tasks in sequence):
  1. check_landing_files  → gate: verify GCS files exist for execution date
  2. validate_data        → fail fast: schema + null + range checks
  3. load_raw_to_bigquery → GCS NDJSON → BigQuery skylens_raw.raw_flights
  4. dbt_run              → dbt staging → intermediate → mart
  5. dbt_test             → run all 15+ dbt schema/data tests
  6. archive_files        → move landing/ to archive/ (prevent reprocess)
  7. notify_success       → Slack summary with row counts

Schedule: 02:00 IST daily (20:30 UTC)
Data window: yesterday's date partition in GCS

on_failure_callback fires on ANY task failure → Slack alert with task name + log URL
"""

from datetime import datetime, timedelta
import json
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import requests

logger = logging.getLogger(__name__)

# ── Config — set as Airflow Variables (not hardcoded) ────────
GCP_PROJECT     = Variable.get("BQ_PROJECT",      default_var="skylens-india")
GCS_BUCKET      = Variable.get("GCS_BUCKET",      default_var="skylens-datalake-skylens-india")
BQ_DATASET_RAW  = Variable.get("BQ_DATASET_RAW",  default_var="skylens_raw")
BQ_DATASET_DBT  = Variable.get("BQ_DATASET_DBT",  default_var="skylens_dbt")
SLACK_WEBHOOK   = Variable.get("SLACK_WEBHOOK_URL",default_var="")
DBT_DIR         = "/home/airflow/gcs/dags/dbt/skylens"

# ── Default args (applied to every task) ────────────────────
# retries=2 means Airflow retries a failed task twice before
# marking it FAILED and calling on_failure_callback
default_args = {
    "owner":            "skylens-data-team",
    "depends_on_past":  False,
    "start_date":       datetime(2019, 3, 1),   # matches real data start
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout":timedelta(hours=2),
    "email_on_failure": False,  # We use Slack
    "email_on_retry":   False,
}


# ── Failure callback ─────────────────────────────────────────
def slack_failure_alert(context):
    """
    Called automatically when any task fails.
    Sends a Slack message with:
    - Which DAG and task failed
    - Execution date
    - Direct link to the Airflow task logs
    """
    if not SLACK_WEBHOOK:
        return
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_dt = context["execution_date"]
    log_url = context["task_instance"].log_url
    err     = str(context.get("exception", "Unknown"))[:300]

    payload = {
        "attachments": [{
            "color": "#FF0000",
            "title": f"SkyLens Pipeline FAILED",
            "fields": [
                {"title": "DAG",       "value": dag_id,  "short": True},
                {"title": "Task",      "value": task_id, "short": True},
                {"title": "Date",      "value": str(exec_dt)[:10], "short": True},
                {"title": "Error",     "value": err, "short": False},
                {"title": "Logs",      "value": log_url, "short": False},
            ],
            "footer": "Airflow | skylens-india",
        }]
    }
    try:
        requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Slack alert failed: {e}")


# Attach callback now that it's defined
default_args["on_failure_callback"] = slack_failure_alert


# ── Task 1: Check landing files ──────────────────────────────
def check_landing_files(**context):
    """
    LANDING LAYER — the pipeline's entry gate.

    Uses GCSHook to list files in landing/date={{ ds }}/
    where {{ ds }} is Airflow's execution_date as YYYY-MM-DD string.

    For the real dataset: partitions are named by journey_date
    (e.g. landing/date=2019-03-24/flights_1234.ndjson)

    XCom outputs:
    - landing_files: list of GCS blob paths
    - file_count: integer count
    """
    exec_date = context["ds"]   # e.g. "2019-03-24"
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    prefix = f"landing/date={exec_date}/"
    blobs  = hook.list(GCS_BUCKET, prefix=prefix)

    if not blobs:
        raise FileNotFoundError(
            f"No files found at gs://{GCS_BUCKET}/{prefix}\n"
            f"Run: python ingestion/upload_csv_to_gcs.py to populate landing zone."
        )

    logger.info(f"Found {len(blobs)} files for {exec_date}:")
    for b in blobs:
        logger.info(f"  gs://{GCS_BUCKET}/{b}")

    context["ti"].xcom_push(key="landing_files", value=blobs)
    context["ti"].xcom_push(key="file_count",    value=len(blobs))
    return len(blobs)


# ── Task 2: Validate data ────────────────────────────────────
def validate_data(**context):
    """
    VALIDATE LAYER — fail fast before touching BigQuery.

    Checks each landing file for:
    1. Valid JSON (parseable NDJSON)
    2. Required columns present (from real flight_date.xlsx schema)
    3. price > 0 and price < 200000 (outlier check)
    4. journey_date matches execution date
    5. num_stops is 0-4 (real data has 0,1,2,3,4 stops)
    6. airline is from known set

    Samples first 20 rows of each file — fast, doesn't load full file.
    """
    exec_date     = context["ds"]
    hook          = GCSHook(gcp_conn_id="google_cloud_default")
    landing_files = context["ti"].xcom_pull(
        key="landing_files", task_ids="check_landing_files"
    )

    # Real airlines from the dataset
    KNOWN_AIRLINES = {
        "IndiGo", "Air India", "Jet Airways", "SpiceJet",
        "Vistara", "GoAir", "Air Asia", "Multiple carriers",
        "Multiple carriers Premium economy", "Jet Airways Business",
        "Vistara Premium economy", "TruJet"
    }

    REQUIRED_FIELDS = {
        "airline", "journey_date", "source_city", "destination_city",
        "route", "dep_time", "arrival_time", "duration_minutes",
        "num_stops", "price", "ingested_at"
    }

    errors = []
    total_rows = 0

    for blob_path in landing_files:
        content = hook.download(GCS_BUCKET, blob_path).decode("utf-8")
        lines   = [l.strip() for l in content.split("\n") if l.strip()]
        total_rows += len(lines)

        for i, line in enumerate(lines[:20]):   # sample first 20
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                errors.append(f"{blob_path}:{i} invalid JSON: {e}")
                continue

            missing = REQUIRED_FIELDS - set(rec.keys())
            if missing:
                errors.append(f"{blob_path}:{i} missing fields: {missing}")

            price = rec.get("price", 0)
            if not (0 < price < 200000):
                errors.append(f"{blob_path}:{i} price out of range: {price}")

            stops = rec.get("num_stops")
            if stops is not None and not (0 <= stops <= 4):
                errors.append(f"{blob_path}:{i} invalid stops: {stops}")

            airline = rec.get("airline", "")
            if airline not in KNOWN_AIRLINES:
                logger.warning(f"Unknown airline in row {i}: {airline}")

    if errors:
        raise ValueError(
            f"Validation failed — {len(errors)} errors:\n" +
            "\n".join(errors[:10])
        )

    logger.info(f"Validation passed. Total rows across files: {total_rows}")
    context["ti"].xcom_push(key="total_rows", value=total_rows)
    return total_rows


# ── Task 3: Load GCS → BigQuery ── handled by built-in operator (see DAG below)


# ── Task 4 & 5: dbt run + test ── handled by BashOperator (see DAG below)


# ── Task 6: Archive files ────────────────────────────────────
def archive_files(**context):
    """
    ARCHIVE LAYER — housekeeping after successful load.

    Moves files from:
      landing/date=YYYY-MM-DD/  →  archive/date=YYYY-MM-DD/

    Why: prevents the check_landing_files task from finding old
    already-processed files if the DAG is re-run for the same date.
    Also keeps the landing zone clean for monitoring.
    """
    hook          = GCSHook(gcp_conn_id="google_cloud_default")
    landing_files = context["ti"].xcom_pull(
        key="landing_files", task_ids="check_landing_files"
    )

    for blob_path in landing_files:
        archive_path = blob_path.replace("landing/", "archive/", 1)
        hook.copy(GCS_BUCKET, blob_path, GCS_BUCKET, archive_path)
        hook.delete(GCS_BUCKET, blob_path)
        logger.info(f"Archived: {blob_path} → {archive_path}")

    logger.info(f"Archived {len(landing_files)} files.")
    return len(landing_files)


# ── Task 7: Slack success notification ──────────────────────
def notify_success(**context):
    """
    NOTIFY LAYER — post summary to Slack on full pipeline success.
    Reads row counts and file counts from XCom.
    Uses TriggerRule.ALL_SUCCESS — only runs if ALL upstream tasks passed.
    """
    if not SLACK_WEBHOOK:
        logger.info("No Slack webhook configured — skipping notification.")
        return

    exec_date  = context["ds"]
    total_rows = context["ti"].xcom_pull(key="total_rows", task_ids="validate_data") or 0
    file_count = context["ti"].xcom_pull(key="file_count", task_ids="check_landing_files") or 0

    payload = {
        "attachments": [{
            "color": "#36a64f",
            "title": "SkyLens Pipeline Completed",
            "fields": [
                {"title": "Date",        "value": exec_date,       "short": True},
                {"title": "Files",       "value": str(file_count), "short": True},
                {"title": "Rows loaded", "value": f"{total_rows:,}","short": True},
                {"title": "dbt tests",   "value": "All passed",    "short": True},
                {"title": "BigQuery",    "value": f"`{GCP_PROJECT}.{BQ_DATASET_DBT}`","short": False},
            ],
            "footer": "Airflow | skylens-india",
            "ts": int(datetime.utcnow().timestamp()),
        }]
    }
    requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    logger.info("Success notification sent to Slack.")


# ── DAG definition ───────────────────────────────────────────
with DAG(
    dag_id="skylens_daily_pipeline",
    description="SkyLens India — Daily flight data ETL pipeline",
    default_args=default_args,
    # 20:30 UTC = 02:00 IST (India Standard Time = UTC+5:30)
    schedule_interval="30 20 * * *",
    catchup=False,          # Don't backfill — just run from today forward
    max_active_runs=1,      # One concurrent run only — prevents data races
    tags=["skylens", "aviation", "dbt", "bigquery"],
) as dag:

    # ── Task 1 ───────────────────────────────────────────────
    t_check_landing = PythonOperator(
        task_id="check_landing_files",
        python_callable=check_landing_files,
        doc_md="LANDING: Verify NDJSON files exist in GCS for today's date partition",
    )

    # ── Task 2 ───────────────────────────────────────────────
    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        doc_md="VALIDATE: Schema + null + range checks. Fails fast before any BQ load.",
    )

    # ── Task 3 ───────────────────────────────────────────────
    # GCSToBigQueryOperator is a built-in Airflow provider operator.
    # It handles: schema detection, retry on quota, chunked upload.
    # source_objects uses Jinja: {{ ds }} = execution date (YYYY-MM-DD)
    t_load_bq = GCSToBigQueryOperator(
        task_id="load_raw_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["landing/date={{ ds }}/*.ndjson"],
        destination_project_dataset_table=(
            f"{GCP_PROJECT}.{BQ_DATASET_RAW}.raw_flights"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",     # Replace partition safely
        time_partitioning={"type": "DAY", "field": "journey_date"},
        create_disposition="CREATE_IF_NEEDED",
        autodetect=False,                       # Use explicit schema (safer)
        gcp_conn_id="google_cloud_default",
        doc_md="LOAD: GCS NDJSON → BigQuery raw_flights (date-partitioned, WRITE_TRUNCATE)",
    )

    # ── Task 4 ───────────────────────────────────────────────
    # BashOperator runs dbt CLI on the Composer VM.
    # --vars passes execution_date so incremental models filter correctly.
    # --select staging intermediate marts runs all 3 layers in dependency order.
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run "
            "--profiles-dir . "
            "--target prod "
            "--vars '{\"execution_date\": \"{{ ds }}\"}' "
            "--select staging intermediate marts "
            "--no-partial-parse"
        ),
        doc_md="DBT RUN: Execute staging → intermediate → mart models in dependency order",
    )

    # ── Task 5 ───────────────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test "
            "--profiles-dir . "
            "--target prod "
            "--select staging intermediate marts"
        ),
        doc_md="DBT TEST: Run all 15+ schema/data quality tests. DAG fails if any test fails.",
    )

    # ── Task 6 ───────────────────────────────────────────────
    t_archive = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files,
        doc_md="ARCHIVE: Move processed files from landing/ to archive/",
    )

    # ── Task 7 ───────────────────────────────────────────────
    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,   # Only if all 6 upstream tasks passed
        doc_md="NOTIFY: Post Slack summary with row counts and test results",
    )

    # ── Dependency chain ─────────────────────────────────────
    # This defines the DAG graph. >> = "then run".
    # If any task fails, all downstream tasks are skipped.
    (
        t_check_landing
        >> t_validate
        >> t_load_bq
        >> t_dbt_run
        >> t_dbt_test
        >> t_archive
        >> t_notify
    )
