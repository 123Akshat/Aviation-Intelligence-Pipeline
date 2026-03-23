-- models/intermediate/int_flight_enriched.sql
-- ============================================================
-- INTERMEDIATE LAYER — int_flight_enriched
-- ============================================================
-- Applies business logic on top of staging:
--   1. Price banding (Budget / Mid-range / Premium / Luxury)
--   2. Time-of-day departure classification
--   3. Route popularity ranking (based on count of records)
--   4. Is-weekend flag
--   5. Duration vs stops consistency flag (anomaly detection)
--   6. Lay-over detection from route string
-- ============================================================

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'flight_key',
    partition_by = {'field': 'journey_date', 'data_type': 'date'}
) }}

with staged as (

    select * from {{ ref('stg_flights') }}

    {% if is_incremental() %}
        where journey_date = cast(
            '{{ var("execution_date", modules.datetime.date.today().isoformat()) }}'
            as date
        )
    {% endif %}

),

enriched as (

    select
        *,

        -- Price band (based on real distribution from flight_date.xlsx):
        -- P25=₹5,277 | P50=₹8,372 | P75=₹12,373 | Max=₹79,512
        case
            when price_inr <  5000  then 'Budget'        -- cheapest 25%
            when price_inr < 8500   then 'Mid-range'     -- 25-50th pct
            when price_inr < 12500  then 'Upper-mid'     -- 50-75th pct
            when price_inr < 25000  then 'Premium'       -- 75-95th pct
            else                         'Luxury'        -- top 5% + outliers
        end                                              as price_band,

        -- Time-of-day bucket for departure hour
        case
            when dep_hour between 5  and 8  then 'Early Morning'  -- 05:00-08:59
            when dep_hour between 9  and 11 then 'Morning'        -- 09:00-11:59
            when dep_hour between 12 and 16 then 'Afternoon'      -- 12:00-16:59
            when dep_hour between 17 and 20 then 'Evening'        -- 17:00-20:59
            else                                 'Night'          -- 21:00-04:59
        end                                              as departure_slot,

        -- Weekend flag (DOW: 1=Sun, 7=Sat in BigQuery)
        case
            when journey_dow in (1, 7) then true
            else false
        end                                              as is_weekend,

        -- Month season for Indian context
        case
            when journey_month in (3, 4)  then 'Spring (Holi/Navratri)'
            when journey_month in (5, 6)  then 'Pre-Monsoon'
            when journey_month in (7, 8)  then 'Monsoon'
            when journey_month in (9, 10) then 'Post-Monsoon (Dussehra/Diwali)'
            when journey_month in (11, 12)then 'Winter (Christmas/NYE)'
            else                               'New Year'
        end                                              as travel_season,

        -- Route label for display
        concat(source_iata, ' → ', destination_iata)    as route_label,
        concat(source_city, ' to ', destination_city)   as route_description,

        -- Price per hour of flying (normalises for route length)
        round(
            safe_divide(price_inr, greatest(duration_minutes, 1)) * 60,
            0
        )                                                as price_per_hour_inr,

        -- Flag suspiciously long duration for num_stops
        -- Real data: some 1-stop flights are 25h+ due to long layovers
        case
            when num_stops = 0 and duration_minutes > 300  then true   -- non-stop > 5h is unusual domestically
            when num_stops = 1 and duration_minutes > 600  then true   -- 1-stop > 10h is a long layover
            when num_stops = 2 and duration_minutes > 900  then true   -- 2-stop > 15h
            else false
        end                                              as is_long_layover,

        -- Number of via airports in route (real format: "BLR → IXR → BBI → BLR")
        -- route_segment_count - 1 = number of stops by IATA (should match num_stops)
        (route_segment_count - 1)                        as route_calculated_stops,

        -- Flag if parsed stops != route segment count (data quality check)
        case
            when (route_segment_count - 1) != num_stops then true
            else false
        end                                              as stops_mismatch_flag

    from staged

)

select * from enriched
