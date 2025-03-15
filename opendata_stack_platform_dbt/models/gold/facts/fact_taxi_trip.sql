{{ config(
    materialized='incremental',
    unique_key='trip_id',
    schema='gold',
    full_refresh=true
) }}

-- Yellow taxi trips
with yellow_trips as (
    select
        -- Source identifier
        'yellow' as taxi_type,

        -- Use row_hash from source as trip_id
        row_hash as trip_id,

        -- Dimension keys will be joined later
        vendor_id,
        cast(ratecode_id as int) as ratecode_id,
        cast(payment_type as int) as payment_type_id,
        null as trip_type_id, -- Yellow taxis don't have trip type

        -- Date and time fields for dimension lookups
        tpep_pickup_datetime,
        tpep_dropoff_datetime,

        -- Format date_key as YYYYMMDD integer - match dim_date format
        cast(
            extract(year from tpep_pickup_datetime) * 10000 +
            extract(month from tpep_pickup_datetime) * 100 +
            extract(day from tpep_pickup_datetime)
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

        cast(
            date_part('hour', tpep_dropoff_datetime) * 3600
            + date_part('minute', tpep_dropoff_datetime) * 60
            as int
        ) as seconds_of_day_dropoff,

        -- Location dimension keys
        pu_location_id,
        do_location_id,

        -- Trip metrics
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

        -- Trip flags
        store_and_fwd_flag,

        -- Timestamp for incremental loads
        tpep_pickup_datetime as incremental_timestamp,

        -- Metadata
        current_timestamp as record_loaded_timestamp
    from {{ source('silver_yellow', 'yellow_taxi_trip') }}
    where 1 = 1
    {% if is_incremental() %}
        -- Match DLT partition strategy (YEAR_MONTH_FORMAT)
        and date_trunc('month', tpep_pickup_datetime)::date >= (
            select coalesce(
                date_trunc('month', max(tpep_pickup_datetime))::date,
                '2000-01-01'::date
            )
            from {{ this }}
            where taxi_type = 'yellow'
        )
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

        -- Dimension keys will be joined later
        vendor_id,
        cast(ratecode_id as int) as ratecode_id,
        cast(payment_type as int) as payment_type_id,
        cast(trip_type as int) as trip_type_id,

        -- Date and time fields for dimension lookups
        lpep_pickup_datetime as tpep_pickup_datetime, -- Standardize column names
        lpep_dropoff_datetime as tpep_dropoff_datetime, -- Standardize column names

        -- Format date_key as YYYYMMDD integer - match dim_date format
        cast(
            extract(year from lpep_pickup_datetime) * 10000 +
            extract(month from lpep_pickup_datetime) * 100 +
            extract(day from lpep_pickup_datetime)
            as int
        ) as date_key_pickup,
        cast(
            extract(year from lpep_dropoff_datetime) * 10000 +
            extract(month from lpep_dropoff_datetime) * 100 +
            extract(day from lpep_dropoff_datetime)
            as int
        ) as date_key_dropoff,

        -- Create matching time values for joining to dim_time
        -- IMPORTANT: Round to minute precision (60 seconds) to match dim_time
        cast(
            date_part('hour', lpep_pickup_datetime) * 3600
            + date_part('minute', lpep_pickup_datetime) * 60
            as int
        ) as seconds_of_day_pickup,

        cast(
            date_part('hour', lpep_dropoff_datetime) * 3600
            + date_part('minute', lpep_dropoff_datetime) * 60
            as int
        ) as seconds_of_day_dropoff,

        -- Location dimension keys
        pu_location_id,
        do_location_id,

        -- Trip metrics
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        0 as airport_fee, -- Green taxis don't have airport fee
        total_amount,

        -- Trip flags
        store_and_fwd_flag,

        -- Timestamp for incremental loads
        lpep_pickup_datetime as incremental_timestamp,

        -- Metadata
        current_timestamp as record_loaded_timestamp
    from {{ source('silver_green', 'green_taxi_trip') }}
    where 1 = 1
    {% if is_incremental() %}
        -- Match DLT partition strategy (YEAR_MONTH_FORMAT)
        and date_trunc('month', lpep_pickup_datetime)::date >= (
            select coalesce(
                date_trunc('month', max(tpep_pickup_datetime))::date,
                '2000-01-01'::date
            )
            from {{ this }}
            where taxi_type = 'green'
        )
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

        -- Dimension keys will be joined later
        hvfhs_license_num as vendor_id, -- Map license number to vendor
        null as ratecode_id, -- FHVHV doesn't have rate code
        null as payment_type_id, -- FHVHV doesn't have payment type
        null as trip_type_id, -- FHVHV doesn't have trip type

        -- Date and time fields for dimension lookups
        pickup_datetime as tpep_pickup_datetime, -- Standardize column names
        dropoff_datetime as tpep_dropoff_datetime, -- Standardize column names

        -- Format date_key as YYYYMMDD integer - match dim_date format
        cast(
            extract(year from pickup_datetime) * 10000 +
            extract(month from pickup_datetime) * 100 +
            extract(day from pickup_datetime)
            as int
        ) as date_key_pickup,
        cast(
            extract(year from dropoff_datetime) * 10000 +
            extract(month from dropoff_datetime) * 100 +
            extract(day from dropoff_datetime)
            as int
        ) as date_key_dropoff,

        -- Create matching time values for joining to dim_time
        -- IMPORTANT: Round to minute precision (60 seconds) to match dim_time
        cast(
            date_part('hour', pickup_datetime) * 3600
            + date_part('minute', pickup_datetime) * 60
            as int
        ) as seconds_of_day_pickup,

        cast(
            date_part('hour', dropoff_datetime) * 3600
            + date_part('minute', dropoff_datetime) * 60
            as int
        ) as seconds_of_day_dropoff,

        -- Location dimension keys
        pu_location_id,
        do_location_id,

        -- Trip metrics
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

        -- Trip flags
        null as store_and_fwd_flag, -- No equivalent in FHVHV

        -- Timestamp for incremental loads
        pickup_datetime as incremental_timestamp,

        -- Metadata
        current_timestamp as record_loaded_timestamp
    from {{ source('silver_fhvhv', 'fhvhv_taxi_trip') }}
    where 1 = 1
    {% if is_incremental() %}
        -- Match DLT partition strategy (YEAR_MONTH_FORMAT)
        and date_trunc('month', pickup_datetime)::date >= (
            select coalesce(
                date_trunc('month', max(tpep_pickup_datetime))::date,
                '2000-01-01'::date
            )
            from {{ this }}
            where taxi_type = 'fhvhv'
        )
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
        c.trip_id,
        c.date_key_pickup,
        c.date_key_dropoff,
        c.time_key_pickup,
        c.time_key_dropoff,

        -- Join to dimension tables to get surrogate keys
        v.vendor_key,
        r.rate_code_key,
        p.payment_type_key,
        t.trip_type_key,

        -- Use coalesce to map invalid locations to unknown location (-1)
        coalesce(pu_loc.location_key, -1) as pu_location_key,
        coalesce(do_loc.location_key, -1) as do_location_key,

        -- Keep original location IDs for reference
        c.pu_location_id,
        c.do_location_id,

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
        c.tpep_pickup_datetime,
        c.tpep_dropoff_datetime,

        -- Metadata
        c.incremental_timestamp,
        c.record_loaded_timestamp,

        -- Generate a surrogate key for the fact table as a STRING type
        -- Use taxi_type, trip_id, and pickup time to ensure uniqueness
        cast(
            {{
                dbt_utils.generate_surrogate_key([
                    'c.taxi_type',
                    'c.trip_id',
                    'c.tpep_pickup_datetime'
                ])
            }} as varchar
        ) as taxi_trip_key
    from all_trips c
    left join {{ ref('dim_vendor') }} v
        on c.vendor_id = v.vendor_id
    left join {{ ref('dim_rate_code') }} r
        on c.ratecode_id = r.rate_code_id
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
