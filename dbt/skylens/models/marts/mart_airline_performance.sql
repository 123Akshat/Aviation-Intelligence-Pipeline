-- models/marts/mart_airline_performance.sql
-- ============================================================
-- MART LAYER — mart_airline_performance
-- ============================================================
-- Grain: one row per airline + month
-- Used for: airline comparison in Looker Studio dashboard
--
-- Answers:
--   - Which airline is cheapest on average?
--   - Which has the most non-stop flights?
--   - How does price vary by carrier type (LCC vs FSC)?
-- ============================================================

{{ config(materialized='table') }}

with enriched as (

    select * from {{ ref('int_flight_enriched') }}

),

airline_monthly as (

    select
        airline_clean,
        airline_raw,
        airline_category,
        journey_month,
        journey_day_name,

        -- Volume
        count(*)                                         as total_flights,
        count(case when num_stops = 0 then 1 end)        as nonstop_count,
        count(case when num_stops = 1 then 1 end)        as one_stop_count,
        count(case when num_stops >= 2 then 1 end)       as multi_stop_count,

        round(100.0 * count(case when num_stops = 0 then 1 end) / count(*), 2)
                                                         as nonstop_pct,

        -- Price metrics (INR)
        round(avg(price_inr), 0)                         as avg_price_inr,
        min(price_inr)                                   as min_price_inr,
        max(price_inr)                                   as max_price_inr,
        round(stddev(price_inr), 0)                      as price_stddev,

        -- Price by stop count
        round(avg(case when num_stops = 0 then price_inr end), 0) as avg_price_nonstop,
        round(avg(case when num_stops = 1 then price_inr end), 0) as avg_price_1stop,
        round(avg(case when num_stops >= 2 then price_inr end), 0) as avg_price_multistop,

        -- Duration
        round(avg(duration_minutes), 0)                  as avg_duration_min,
        min(duration_minutes)                            as min_duration_min,
        max(duration_minutes)                            as max_duration_min,

        -- Price efficiency
        round(avg(price_per_hour_inr), 0)                as avg_price_per_hour,

        -- Additional info breakdown
        countif(additional_info = 'No Info')             as no_info_count,
        countif(additional_info = 'In-flight meal not included') as no_meal_count,
        countif(additional_info = 'No check-in baggage included') as no_baggage_count

    from enriched
    group by 1, 2, 3, 4, 5

)

select
    *,
    -- Share of multi-stop flights
    round(100.0 * multi_stop_count / nullif(total_flights, 0), 2) as multistop_pct
from airline_monthly
order by journey_month, total_flights desc
