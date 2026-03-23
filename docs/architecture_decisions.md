# Architecture Decision Records (ADR)

## ADR-001: Why GCS as the data lake layer?

**Decision:** Store raw NDJSON files in GCS before loading to BigQuery.

**Reason:** Decouples ingestion from loading. If BigQuery has a quota issue or the schema changes,
the raw data is safely in GCS and can be reloaded without re-running ingestion. GCS also acts as an
audit trail — you can always trace what raw data produced a given BigQuery row.

**Alternative considered:** Load directly from pandas/Python to BigQuery (no GCS). Rejected because
it creates no recovery point if the load fails partway through.

---

## ADR-002: Why date-partition BigQuery tables by journey_date?

**Decision:** Partition raw_flights and all incremental dbt models on journey_date (DATE).

**Reason:** The pipeline runs daily and processes one day's data per run. With partitioning, each
daily query scans only that day's partition rather than the full table. At 10,683 rows this is
negligible, but at 10 million rows (one year of all Indian domestic flights) it reduces query cost
by 99%+.

**BigQuery cost model:** You pay per bytes scanned. Partition pruning = fewer bytes = lower cost.

---

## ADR-003: Why three dbt layers instead of one big SQL file?

**Decision:** Staging → Intermediate → Mart with strict separation of concerns.

**Reason:**
- Staging: owned by the data engineer. If source schema changes, only staging is updated.
- Intermediate: owned by analytics engineers. Business logic lives here, reused by all marts.
- Mart: owned by the BI team. Optimised for the dashboard tool. Never contains raw joins.

**Alternative considered:** Single transformation SQL. Rejected because changing one business rule
would require updating multiple mart files, increasing bug risk.

---

## ADR-004: Why Airflow instead of Cloud Functions or Cloud Run Jobs?

**Decision:** Use Apache Airflow on Cloud Composer for orchestration.

**Reason:** The pipeline has 7 dependent tasks with data passing between them (XCom), conditional
logic (TriggerRule.ALL_SUCCESS on notify), retry logic, and scheduled execution. Airflow handles all
of these natively. Cloud Functions are stateless event-driven — not suited for multi-step pipelines
with dependencies.

**Trade-off:** Cloud Composer costs ~$300-400/month minimum (Kubernetes cluster). For a portfolio
project, Cloud Run + Cloud Scheduler would be cheaper. Composer was chosen to demonstrate
production-grade orchestration patterns.

---

## ADR-005: Why WRITE_TRUNCATE on BigQuery partition (not WRITE_APPEND)?

**Decision:** Use WRITE_TRUNCATE when loading daily partitions.

**Reason:** Makes the pipeline idempotent. If today's DAG fails after partially loading data and
you re-run it, WRITE_TRUNCATE replaces the partition cleanly — no duplicate rows. WRITE_APPEND
would double-count every rerun.

**Rule of thumb:** Idempotency is the most important property of a production data pipeline.

---

## ADR-006: Why parse Duration and Stops in Python (ingestion) not in dbt (staging)?

**Decision:** Heavy format parsing (string → integer) happens in upload_csv_to_gcs.py. Light
standardisation (city names, airline normalisation) happens in stg_flights.sql.

**Reason:** BigQuery SQL handles string-to-integer casting cleanly. But the regex parsing of
"2h 50m" → 170 and "non-stop" → 0 is more readable and testable in Python. Keeping complex
parsing in Python also means dbt staging can be pure SQL without embedded regex.

**Alternative:** Use dbt's regex_extract macros in staging. Rejected because regex in Jinja SQL
is harder to unit test than Python functions.
