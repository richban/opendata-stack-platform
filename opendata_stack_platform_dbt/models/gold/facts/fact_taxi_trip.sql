{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "date_partition",
        "data_type": "date",
        "granularity": "month"
    }
) }}

-- Yellow taxi trips
with yellow_trips as (
    select
        -- Source identifier
        'yellow' as taxi_type,

        -- Use row_hash from source as trip_id
        row_hash as trip_id,

        -- Simple direct references first
        vendor_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        store_and_fwd_flag,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        tpep_pickup_datetime as incremental_timestamp,

        -- Dimension keys will be joined later
        -- default to 99 if null (unknown)
        coalesce(ratecode_id, 99) as rate_code_id,
        -- payment_type 0 is unknown
        -- Default to 5 if payment_type is null
        case
            when payment_type = 0 then 5
            else coalesce(payment_type, 5)
        end as payment_type_id,
        -- Yellow taxis don't have trip type but by law it's street-hail (1 = Street-hail)
        1 as trip_type_id,

        -- Date and time fields for dimension lookups
        -- Format date_key as YYYYMMDD integer - match dim_date format
        cast(
            extract(year from tpep_pickup_datetime) * 10000
            + extract(month from tpep_pickup_datetime) * 100
            + extract(day from tpep_pickup_datetime)
            as int
        ) as date_key_pickup,
        cast(
            date_part('year', tpep_dropoff_datetime) * 10000
            + date_part('month', tpep_dropoff_datetime) * 100
            + date_part('day', tpep_dropoff_datetime) as int
        ) as date_key_dropoff,

        -- Create matching time values for joining to dim_time
        -- IMPORTANT: Round to minute precision (60 seconds) to match dim_time
        cast(
            date_part('hour', tpep_pickup_datetime) * 3600
            + date_part('minute', tpep_pickup_datetime) * 60
            as int
        ) as seconds_of_day_pickup,

        -- Calculate seconds of day from dropoff datetime (0-86399)
        cast(
            date_part('hour', tpep_dropoff_datetime) * 3600
            + date_part('minute', tpep_dropoff_datetime) * 60
            as int
        ) as seconds_of_day_dropoff,

        -- Partition field for delete+insert strategy
        date_trunc('month', tpep_pickup_datetime) as date_partition,

        -- Metadata
        current_timestamp as record_loaded_timestamp
    from {{ source('silver_yellow', 'yellow_taxi_trip') }}
    where
        1 = 1
        {% if is_incremental() and not var('backfill_start_date', false) %}
            -- Normal incremental behavior - only process new data
            and date_trunc('month', tpep_pickup_datetime) >= (
                select
                    coalesce(
                        date_trunc('month', max(incremental_timestamp)),
                        '2000-01-01'::date
                    )
                from {{ this }}
                where taxi_type = 'yellow'
            )
        {% elif is_incremental() and var('backfill_start_date', false) %}
            -- Backfill behavior - process specified date range
            and date_trunc('month', tpep_pickup_datetime) >= cast('{{ var("backfill_start_date") }}' as date)
            and date_trunc('month', tpep_pickup_datetime) <= cast('{{ var("backfill_end_date") }}' as date)
        {% endif %}
),

-- Get time_key for pickup and dropoff
yellow_with_time_keys as (
    select
        y.*,
        t_pickup.time_key as time_key_pickup,
        t_dropoff.time_key as time_key_dropoff
    from yellow_trips y
    left join {{ ref('dim_time') }} t_pickup
        on y.seconds_of_day_pickup = t_pickup.seconds_of_day
    left join {{ ref('dim_time') }} t_dropoff
        on y.seconds_of_day_dropoff = t_dropoff.seconds_of_day
),

-- Green taxi trips
green_trips as (
    select
        -- Source identifier
        'green' as taxi_type,
        -- Use row_hash from source as trip_id
        row_hash as trip_id,

        -- Simple direct references first
        vendor_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        null as airport_fee,
        total_amount,
        store_and_fwd_flag,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        lpep_pickup_datetime as incremental_timestamp,

        -- Dimension keys will be joined later
        -- default to 99 if null (unknown)
        coalesce(ratecode_id, 99) as rate_code_id,
        case
            when payment_type = 0 then 5 -- payment_type 0 is unknown
            else coalesce(payment_type, 5) -- Default to 5 if payment_type is null
        end as payment_type_id,
        trip_type as trip_type_id,

        -- Format date_key as YYYYMMDD integer - match dim_date format
        cast(
            extract(year from lpep_pickup_datetime) * 10000
            + extract(month from lpep_pickup_datetime) * 100
            + extract(day from lpep_pickup_datetime)
            as int
        ) as date_key_pickup,
        cast(
            extract(year from lpep_dropoff_datetime) * 10000
            + extract(month from lpep_dropoff_datetime) * 100
            + extract(day from lpep_dropoff_datetime)
            as int
        ) as date_key_dropoff,

        -- Create matching time values for joining to dim_time
        -- IMPORTANT: Round to minute precision (60 seconds) to match dim_time
        cast(
            date_part('hour', lpep_pickup_datetime) * 3600
            + date_part('minute', lpep_pickup_datetime) * 60
            as int
        ) as seconds_of_day_pickup,

        -- Calculate seconds of day from dropoff datetime (0-86399)
        cast(
            date_part('hour', lpep_dropoff_datetime) * 3600
            + date_part('minute', lpep_dropoff_datetime) * 60
            as int
        ) as seconds_of_day_dropoff,

        -- Partition field for delete+insert strategy
        date_trunc('month', lpep_pickup_datetime) as date_partition,
        -- Metadata
        current_timestamp as record_loaded_timestamp
    from {{ source('silver_green', 'green_taxi_trip') }}
    where
        1 = 1
        {% if is_incremental() and not var('backfill_start_date', false) %}
            -- Normal incremental behavior - only process new data
            and date_trunc('month', lpep_pickup_datetime) >= (
                select
                    coalesce(
                        date_trunc('month', max(incremental_timestamp)),
                        '2000-01-01'::date
                    )
                from {{ this }}
                where taxi_type = 'green'
            )
        {% elif is_incremental() and var('backfill_start_date', false) %}
            -- Backfill behavior - process specified date range
            and date_trunc('month', lpep_pickup_datetime) >= cast('{{ var("backfill_start_date") }}' as date)
            and date_trunc('month', lpep_pickup_datetime) <= cast('{{ var("backfill_end_date") }}' as date)
        {% endif %}
),

-- Get time_key for pickup and dropoff
green_with_time_keys as (
    select
        g.*,
        t_pickup.time_key as time_key_pickup,
        t_dropoff.time_key as time_key_dropoff
    from green_trips g
    left join {{ ref('dim_time') }} t_pickup
        on g.seconds_of_day_pickup = t_pickup.seconds_of_day
    left join {{ ref('dim_time') }} t_dropoff
        on g.seconds_of_day_dropoff = t_dropoff.seconds_of_day
),

-- High-Volume For-Hire Vehicle (HVFHV) trips
fhvhv_trips as (
    select
        -- Source identifier
        'fhvhv' as taxi_type,
        -- Use row_hash from source as trip_id
        row_hash as trip_id,

        -- Simple direct references first
        case
            when lower(hvfhs_license_num) like 'HV%'
                then cast(replace(hvfhs_license_num, 'HV', '') as int)
            else -1 -- Default for unknown pattern
        end as vendor_id,
        pu_location_id,
        do_location_id,
        null as passenger_count, -- FHVHV doesn't have passenger count
        trip_miles as trip_distance, -- Map miles to distance
        base_passenger_fare as fare_amount, -- Map base fare
        null as extra, -- FHVHV doesn't have extra
        null as mta_tax, -- FHVHV doesn't have MTA tax
        tips as tip_amount, -- Map tips
        tolls as tolls_amount, -- Map tolls
        null as improvement_surcharge, -- FHVHV doesn't have improvement surcharge
        congestion_surcharge,
        null as airport_fee, -- FHVHV doesn't have airport fee
        (base_passenger_fare + tips + tolls + congestion_surcharge) as total_amount,
        null as store_and_fwd_flag, -- No equivalent in FHVHV
        pickup_datetime,
        dropoff_datetime,
        pickup_datetime as incremental_timestamp,

        -- Dimension keys will be joined later
        99 as rate_code_id, -- FHVHV doesn't have rate code (99 = unknown)
        5 as payment_type_id, -- FHVHV doesn't have payment type so use 5 (unknown)
        3 as trip_type_id, -- FHVHV doesn't have trip type. Uber and Lyft are classified as e-dispatch services in New York City.

        -- Format date_key as YYYYMMDD integer - match dim_date format
        cast(
            extract(year from pickup_datetime) * 10000
            + extract(month from pickup_datetime) * 100
            + extract(day from pickup_datetime)
            as int
        ) as date_key_pickup,
        cast(
            extract(year from dropoff_datetime) * 10000
            + extract(month from dropoff_datetime) * 100
            + extract(day from dropoff_datetime)
            as int
        ) as date_key_dropoff,

        -- Calculate seconds of day from pickup datetime (0-86399)
        -- We'll use this to join to the time dimension
        cast(
            date_part('hour', pickup_datetime) * 3600
            + date_part('minute', pickup_datetime) * 60
            as int
        ) as seconds_of_day_pickup,

        -- Calculate seconds of day from dropoff datetime (0-86399)
        cast(
            date_part('hour', dropoff_datetime) * 3600
            + date_part('minute', dropoff_datetime) * 60
            as int
        ) as seconds_of_day_dropoff,

        -- Partition field for delete+insert strategy
        date_trunc('month', pickup_datetime)::date as date_partition,
        -- Metadata
        current_timestamp as record_loaded_timestamp
    from {{ source('silver_fhvhv', 'fhvhv_taxi_trip') }}
    where
        1 = 1
        {% if is_incremental() and not var('backfill_start_date', false) %}
            -- Normal incremental behavior - only process new data
            and date_trunc('month', pickup_datetime) >= (
                select
                    coalesce(
                        date_trunc('month', max(incremental_timestamp))::date,
                        '2000-01-01'::date
                    )
                from {{ this }}
                where taxi_type = 'fhvhv'
            )
        {% elif is_incremental() and var('backfill_start_date', false) %}
            -- Backfill behavior - process specified date range
            and date_trunc('month', pickup_datetime) >= cast('{{ var("backfill_start_date") }}' as date)
            and date_trunc('month', pickup_datetime) <= cast('{{ var("backfill_end_date") }}' as date)
        {% endif %}
),

-- Get time_key for pickup and dropoff
fhvhv_with_time_keys as (
    select
        f.*,
        t_pickup.time_key as time_key_pickup,
        t_dropoff.time_key as time_key_dropoff
    from fhvhv_trips f
    left join {{ ref('dim_time') }} t_pickup
        on f.seconds_of_day_pickup = t_pickup.seconds_of_day
    left join {{ ref('dim_time') }} t_dropoff
        on f.seconds_of_day_dropoff = t_dropoff.seconds_of_day
),

-- Combine all taxi trips
all_trips as (
    select * from yellow_with_time_keys
    union all
    select * from green_with_time_keys
    union all
    select * from fhvhv_with_time_keys
),

-- Final select with dimension key lookups
final as (
    select
        c.taxi_type,
        c.trip_id, -- natural key
        c.date_partition, -- partition field for delete+insert
        c.date_key_pickup,
        c.date_key_dropoff,
        c.time_key_pickup,
        c.time_key_dropoff,

        -- Foreign keys from dimensions
        coalesce(v.vendor_key, -1) as vendor_key,
        r.rate_code_key,
        p.payment_type_key,
        t.trip_type_key,

        -- Use coalesce to map invalid locations to unknown location (264)
        coalesce(pu_loc.location_key, 264) as pu_location_key,
        coalesce(do_loc.location_key, 264) as do_location_key,

        -- Trip metrics
        c.passenger_count,
        c.trip_distance,
        c.fare_amount,
        c.extra,
        c.mta_tax,
        c.tip_amount,
        c.tolls_amount,
        c.improvement_surcharge,
        c.congestion_surcharge,
        c.airport_fee,
        c.total_amount,
        c.store_and_fwd_flag,

        -- Timestamps
        c.pickup_datetime,
        c.dropoff_datetime,

        -- Metadata
        c.incremental_timestamp,
        c.record_loaded_timestamp,

        -- Generate a surrogate key for the fact table as a STRING type
        -- Use taxi_type, trip_id, and pickup time to ensure uniqueness
        {{
            dbt_utils.generate_surrogate_key([
                'c.taxi_type',
                'c.trip_id',
                'c.pickup_datetime'
            ])
        }} as taxi_trip_key
    from all_trips c
    left join {{ ref('dim_vendor') }} v
        on c.vendor_id = v.vendor_id
    left join {{ ref('dim_rate_code') }} r
        on c.rate_code_id = r.rate_code_id
    left join {{ ref('dim_payment_type') }} p
        on c.payment_type_id = p.payment_type_id
    left join {{ ref('dim_trip_type') }} t
        on c.trip_type_id = t.trip_type_id
    -- Join to location dimension, invalid locations will get NULL which we coalesce to -1
    left join {{ ref('dim_location') }} pu_loc
        on c.pu_location_id = pu_loc.location_id
    left join {{ ref('dim_location') }} do_loc
        on c.do_location_id = do_loc.location_id
)

select * from final
