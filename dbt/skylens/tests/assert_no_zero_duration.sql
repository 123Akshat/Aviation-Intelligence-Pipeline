-- tests/assert_no_zero_duration.sql
-- Singular test: no flight should have zero or negative duration
-- (would indicate a parsing failure in upload_csv_to_gcs.py)
-- Returns rows that FAIL the test (dbt expects 0 rows for pass)

select
    flight_key,
    airline_clean,
    source_iata,
    destination_iata,
    duration_minutes,
    journey_date
from {{ ref('stg_flights') }}
where duration_minutes is not null
  and duration_minutes <= 0
