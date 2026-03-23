# SkyLens India — Interview Cheat Sheet

## 30-Second Pitch
"I built SkyLens India — an end-to-end data engineering pipeline on GCP that ingests 10,683 real
Indian domestic flight records, handles real data quality issues like misspelled cities and
mixed-format durations, transforms them through a 3-layer dbt model with 27 automated tests,
and delivers a Looker Studio dashboard showing airline price comparisons, route fare trends,
and non-stop vs 1-stop pricing analysis. The whole pipeline is orchestrated by Airflow on
Cloud Composer, runs daily at 2am IST, and sends Slack alerts if anything breaks."

---

## Key Numbers to Remember
- 10,683 rows | 11 columns | 12 airlines | 5 source cities | 6 destination cities
- Date range: March to June 2019
- Price range: Rs 1,759 to Rs 79,512
- 4,335 rows (40%) have next-day arrival date in Arrival_Time
- 27 dbt data quality tests
- 7 Airflow tasks in sequence
- 3 dbt layers: staging, intermediate, marts (3 tables)
- 2 null rows in the source (Route + Total_Stops — filtered in staging)
- Schedule: 30 20 * * * = 02:00 IST daily

---

## Data Quality Issues (real, from EDA)
| Issue | Column | How Fixed |
|---|---|---|
| "Banglore" misspelling | Source | CASE WHEN in stg_flights.sql |
| Mixed duration strings "2h 50m" vs "19h" | Duration | Regex in upload_csv_to_gcs.py |
| Next-day date in Arrival_Time "01:10 22 Mar" | Arrival_Time | is_next_day_arrival flag |
| Inconsistent date padding "1/05/2019" | Date_of_Journey | pd.to_datetime(dayfirst=True) |
| Airline variants "Jet Airways Business" | Airline | CASE WHEN canonical mapping |
| 2 null rows | Route + Total_Stops | WHERE IS NOT NULL filter |

---

## Airflow Questions
Q: What is catchup=False?
A: Prevents Airflow from running all missed daily intervals from start_date (2019-03-01) to today.
   Without it, hundreds of backfill runs would queue up on first deploy.

Q: What is max_active_runs=1?
A: Prevents two concurrent DAG runs. Stops race conditions on the same BigQuery partition.

Q: What is XCom?
A: Cross-Communication. Lets tasks share small values (file paths, row counts) through
   Airflow's PostgreSQL metadata database. Not for large data — use GCS for that.

Q: What does on_failure_callback do?
A: A Python function called automatically when any task fails. Sends a Slack message with
   DAG ID, task ID, execution date, and a direct link to the Airflow task logs.

Q: What is WRITE_TRUNCATE and why use it?
A: Replaces only the date partition being loaded, not the whole table. Makes reruns idempotent
   — running the same day twice gives the same result, not double the rows.

Q: What is TriggerRule.ALL_SUCCESS?
A: The notify_success task only runs if every upstream task passed. If dbt_test fails,
   notify is skipped and only the failure callback fires.

---

## dbt Questions
Q: Why 3 layers?
A: Staging = contract between raw source and everything else (only casting, no logic).
   Intermediate = shared business logic used by multiple marts.
   Mart = pre-aggregated for BI tools. Separating them means one change doesn't break everything.

Q: What is incremental materialisation?
A: On the first run, builds the full table. Every subsequent run only processes new rows
   (filtered by execution_date). Much faster and cheaper than rebuilding the full table daily.

Q: What is a surrogate key?
A: A generated MD5 hash from multiple columns (airline + date + source + dest + dep_time)
   that uniquely identifies each row. Used as the unique_key for incremental merges.

Q: What is generate_surrogate_key?
A: A dbt_utils macro that creates an MD5 hash from a list of column values.
   Consistent across runs — same input always produces the same hash.

Q: What happens when a dbt test fails?
A: dbt exits with non-zero return code. BashOperator task turns red. Downstream tasks
   (archive, notify_success) are skipped. on_failure_callback fires the Slack alert.

Q: Why pre-aggregate in dbt instead of Looker?
A: Speed (Looker doesn't re-run GROUP BY on every dashboard load) + correctness
   (dbt tests validate the aggregation logic before it reaches the dashboard).

---

## GCP Questions
Q: What is GCS and how is it structured here?
A: Google Cloud Storage — the data lake. 3 logical zones:
   landing/ (raw data arrives), archive/ (processed data), dbt_artifacts/ (build logs).
   Date-partitioned: landing/date=2019-05-01/flights_123.ndjson

Q: What is Pub/Sub?
A: GCP's managed message queue. Decouples the producer (flight data publisher) from
   the consumer (GCS subscriber). Messages retained 7 days so nothing is lost if subscriber is down.

Q: What is Cloud Composer?
A: GCP's fully managed Airflow. Runs on Kubernetes — no server management needed.
   You drop DAG files into a GCS bucket and Composer picks them up automatically.

Q: What is PostgreSQL doing here?
A: It's Airflow's metadata database. Stores DAG run history, task states, XCom values,
   Airflow Variables, and connection configs. NOT where flight data lives.

Q: Why partition BigQuery by journey_date?
A: BigQuery charges per bytes scanned. Partitioning means a WHERE journey_date = X query
   only scans that day's data instead of the full table. At scale = massive cost reduction.

Q: What is clustering in BigQuery?
A: Physical co-location of rows with the same column values on disk.
   stg_flights clusters on [airline_clean, source_iata, destination_iata] so queries
   filtering by airline or route scan far fewer bytes.

---

## Business Insights (have these ready)
1. Non-stop flights cost 20-50% more than 1-stop on the same route
2. Delhi to Kochi is the most popular route (4,537 records = 42% of dataset)
3. Jet Airways had highest volume (3,849 flights) but suspended April 2019
4. IndiGo dominates non-stop short-haul — especially BLR-DEL corridor
5. Night departures (9pm-midnight) tend to be cheaper across most routes
6. Rs 79,512 outlier = Jet Airways Business class, 4-stop BLR-CCU-BBI-HYD-VGA-DEL (29h 30m)

---

## What Would You Add to Make It Production-Ready?
1. Live DGCA API feed instead of static CSV — real-time flight status updates
2. Data lineage with dbt docs deploy — analysts can trace any mart column to raw source
3. Incremental backfill capability — safely reprocess historical partitions after bug fixes
4. Row-level security in BigQuery — restrict airline data access by team
5. dbt snapshots for SCD Type 2 — track how prices change over time for the same flight
6. Cost monitoring alerts — Cloud Monitoring budget alerts if BigQuery spend exceeds threshold
