-- models/marts/mart_route_fares.sql
-- ============================================================
-- MART LAYER — mart_route_fares
-- ============================================================
-- Grain: one row per route + airline + month
-- Answers: which routes are expensive? which airlines are cheapest
-- on the same route? how does price change March → June 2019?
-- ============================================================

{{ config(materialized='table') }}

with enriched as (select * from {{ ref('int_flight_enriched') }}),

route_monthly as (
    select
        source_iata,
        destination_iata,
        route_label,
        route_description,
        airline_clean,
        airline_category,
        stop_category,
        num_stops,
        journey_month,
        is_weekend,
        travel_season,

        count(*)                                         as flight_count,
        round(avg(price_inr), 0)                         as avg_price_inr,
        min(price_inr)                                   as min_price_inr,
        max(price_inr)                                   as max_price_inr,
        round(stddev(price_inr), 0)                      as price_stddev,
        round(avg(duration_minutes), 0)                  as avg_duration_min,
        round(avg(price_per_hour_inr), 0)                as avg_price_per_hour,

        -- Most common additional_info on this route
        approx_top_count(additional_info, 1)[offset(0)].value as most_common_info

    from enriched
    group by 1,2,3,4,5,6,7,8,9,10,11
)

select * from route_monthly
order by source_iata, destination_iata, journey_month, avg_price_inr desc
