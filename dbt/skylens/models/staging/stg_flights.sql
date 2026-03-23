-- models/staging/stg_flights.sql
-- ============================================================
-- STAGING LAYER — stg_flights
-- ============================================================
-- Source:  skylens_raw.raw_flights
--          (10,683 real rows from flight_date.xlsx, loaded by Airflow)
--
-- What this model does:
--   1. Casts all STRING fields to correct types
--   2. Parses duration (string like "2h 50m") → integer minutes
--   3. Standardises stop counts ("non-stop" → 0, "1 stop" → 1)
--   4. Normalises airline names (consolidates "Jet Airways Business" etc.)
--   5. Builds surrogate key
--   6. Filters null route/stops rows (2 real null rows in source)
--
-- NO business logic here — that lives in intermediate.
-- ============================================================

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'flight_key',
    partition_by = {
        'field': 'journey_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by = ['airline_clean', 'source_iata', 'destination_iata']
) }}

with source as (

    select * from {{ source('skylens_raw', 'raw_flights') }}

    {% if is_incremental() %}
        where journey_date = cast(
            '{{ var("execution_date", modules.datetime.date.today().isoformat()) }}'
            as date
        )
    {% endif %}

),

cleaned as (

    select
        -- Surrogate key: unique per flight record
        {{ dbt_utils.generate_surrogate_key([
            'airline', 'date_of_journey_raw', 'source_raw',
            'destination_raw', 'dep_time'
        ]) }} as flight_key,

        -- Airline — normalise variants to canonical names
        -- Real data has: "Jet Airways Business", "Multiple carriers Premium economy" etc.
        case
            when airline like '%Jet Airways%'    then 'Jet Airways'
            when airline like '%Multiple%'       then 'Multiple Carriers'
            when airline like '%Vistara%'        then 'Vistara'
            else trim(airline)
        end                                              as airline_clean,

        trim(airline)                                    as airline_raw,

        -- Classify carrier type (LCC vs Full Service)
        case
            when airline in ('IndiGo','SpiceJet','GoAir','Air Asia','TruJet')
                then 'LCC'
            when airline in ('Air India','Vistara') or airline like '%Jet Airways%'
                then 'FSC'
            when airline like '%Premium%' or airline like '%Business%'
                then 'PREMIUM'
            else 'OTHER'
        end                                              as airline_category,

        -- Journey date (already parsed in ingestion layer as DATE)
        cast(journey_date as date)                       as journey_date,
        extract(month from cast(journey_date as date))  as journey_month,
        extract(dayofweek from cast(journey_date as date)) as journey_dow,
        -- Day of week: 1=Sunday, 2=Monday, ..., 7=Saturday in BigQuery
        format_date('%A', cast(journey_date as date))   as journey_day_name,

        -- Airports — standardised city names from ingestion parsing
        trim(source_city)                                as source_city,
        trim(destination_city)                           as destination_city,
        upper(trim(source_iata))                         as source_iata,
        upper(trim(destination_iata))                    as destination_iata,

        -- Route (e.g. "BLR → DEL" or "CCU → IXR → BBI → BLR")
        trim(route)                                      as route,

        -- Count segments in route = hops in journey
        -- "BLR → DEL" → 1 segment = non-stop
        -- "CCU → IXR → BBI → BLR" → 3 segments = 2 stops
        array_length(split(trim(route), '→'))            as route_segment_count,

        -- Departure time (string "22:20" → just keep as string for now)
        trim(dep_time)                                   as dep_time,
        cast(split(trim(dep_time), ':')[offset(0)] as integer) as dep_hour,

        -- Arrival time (can contain next-day date: "01:10 22 Mar")
        trim(arrival_time)                               as arrival_time,
        cast(has_next_day_arrival as boolean)            as is_next_day_arrival,

        -- Duration in minutes (already parsed in ingestion)
        cast(duration_minutes as integer)                as duration_minutes,
        trim(duration_raw)                               as duration_raw,

        -- Stops (already parsed as integer: 0, 1, 2, 3, 4)
        cast(num_stops as integer)                       as num_stops,
        trim(total_stops_raw)                            as total_stops_raw,

        -- Stop category for cleaner grouping
        case
            when cast(num_stops as integer) = 0 then 'Non-stop'
            when cast(num_stops as integer) = 1 then '1 Stop'
            when cast(num_stops as integer) = 2 then '2 Stops'
            else '3+ Stops'
        end                                              as stop_category,

        -- Additional info — normalise the slight variations
        -- Real data: "No info", "No Info" (capital I variant), actual info strings
        case
            when lower(trim(additional_info)) = 'no info' then 'No Info'
            else trim(additional_info)
        end                                              as additional_info,

        -- Price in INR (already integer in source)
        cast(price as integer)                           as price_inr,

        -- Metadata
        cast(ingested_at as timestamp)                   as ingested_at

    from source

    where
        route is not null          -- drops 1 null route row
        and total_stops_raw is not null  -- drops 1 null stops row
        and journey_date is not null
        and price > 0

)

select * from cleaned
