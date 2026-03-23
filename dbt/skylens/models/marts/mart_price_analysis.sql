-- models/marts/mart_price_analysis.sql
-- ============================================================
-- MART LAYER — mart_price_analysis
-- ============================================================
-- Grain: one row per route + stop_category + departure_slot + price_band
-- Used for: price heatmaps, fare vs stops analysis, time-of-day pricing
--
-- Answers:
--   - Does flying at night save money?
--   - How much more do non-stop flights cost vs 1-stop?
--   - Which routes have highest price variance?
--   - Do weekends cost more?
-- ============================================================

{{ config(materialized='table') }}

with enriched as (

    select * from {{ ref('int_flight_enriched') }}

),

-- Route + stop + time grain
price_by_segment as (

    select
        source_iata,
        destination_iata,
        source_city,
        destination_city,
        route_label,
        route_description,
        airline_clean,
        airline_category,
        stop_category,
        num_stops,
        departure_slot,
        price_band,
        is_weekend,
        travel_season,
        journey_month,
        additional_info,

        count(*)                                          as flight_count,
        round(avg(price_inr), 0)                          as avg_price_inr,
        min(price_inr)                                    as min_price_inr,
        max(price_inr)                                    as max_price_inr,
        round(stddev(price_inr), 0)                       as price_stddev_inr,

        -- Median approximation using percentile
        percentile_cont(price_inr, 0.5) over (
            partition by source_iata, destination_iata, stop_category
        )                                                 as median_price_inr,

        round(avg(duration_minutes), 0)                   as avg_duration_min,
        round(avg(price_per_hour_inr), 0)                 as avg_price_per_hour,

        countif(is_long_layover)                          as long_layover_count

    from enriched
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

),

-- Calculate premium: non-stop price vs 1-stop price on same route
nonstop_vs_onestop as (

    select
        route_label,
        round(avg(case when num_stops = 0 then avg_price_inr end), 0) as nonstop_avg_price,
        round(avg(case when num_stops = 1 then avg_price_inr end), 0) as onestop_avg_price
    from price_by_segment
    group by 1

)

select
    p.*,
    n.nonstop_avg_price,
    n.onestop_avg_price,
    -- Premium you pay for non-stop convenience
    round(
        safe_divide(n.nonstop_avg_price - n.onestop_avg_price, n.onestop_avg_price) * 100,
        1
    )                                                    as nonstop_premium_pct

from price_by_segment p
left join nonstop_vs_onestop n using (route_label)
order by route_label, stop_category, departure_slot
