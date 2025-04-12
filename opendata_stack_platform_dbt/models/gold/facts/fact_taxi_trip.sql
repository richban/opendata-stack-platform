{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='delete+insert',
    partition_by={
        "field": "_date_partition",
        "data_type": "date",
        "granularity": "month"
    }
) }}

with validated_trips as (
    select
        -- Source identifier
        taxi_type,
        trip_id,

        -- Simple direct references
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
        pickup_datetime,
        dropoff_datetime,

        -- Dimension keys
        rate_code_id,
        payment_type_id,
        trip_type_id,

        -- Metadata
        _date_partition,
        _incremental_timestamp,
        _record_loaded_timestamp,

        -- Date and time fields for dimension lookups
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

        -- Create matching time values for joining to dim_time
        -- IMPORTANT: Round to minute precision (60 seconds) to match dim_time
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
        ) as seconds_of_day_dropoff
    from {{ ref('silver_taxi_trips_validated') }}
    where
        1 = 1
        {{ incremental_backfill('pickup_datetime') }}
),

-- Get time_key for pickup and dropoff
trips_with_time_keys as (
    select
        t.*,
        t_pickup.time_key as time_key_pickup,
        t_dropoff.time_key as time_key_dropoff
    from validated_trips t
    left join {{ ref('dim_time') }} t_pickup
        on t.seconds_of_day_pickup = t_pickup.seconds_of_day
    left join {{ ref('dim_time') }} t_dropoff
        on t.seconds_of_day_dropoff = t_dropoff.seconds_of_day
),

-- Final select with dimension key lookups
final as (
    select
        c.taxi_type,
        c.trip_id, -- natural key
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

        -- Timestamps
        c.pickup_datetime,
        c.dropoff_datetime,

        -- Metadata
        c._incremental_timestamp,
        c._record_loaded_timestamp,
        c._date_partition,

        -- Generate a surrogate key for the fact table as a STRING type
        -- Use taxi_type, trip_id, and pickup time to ensure uniqueness
        {{
            dbt_utils.generate_surrogate_key([
                'c.taxi_type',
                'c.trip_id',
                'c.pickup_datetime'
            ])
        }} as taxi_trip_key
    from trips_with_time_keys c
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

select
    taxi_trip_key,
    trip_id, -- natural key

    -- Dimension keys
    taxi_type,
    date_key_pickup,
    date_key_dropoff,
    time_key_pickup,
    time_key_dropoff,

    rate_code_key,
    payment_type_key,
    trip_type_key,
    vendor_key,

    pu_location_key,
    do_location_key,

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

    -- Timestamps
    pickup_datetime,
    dropoff_datetime,

    -- Metadata
    _incremental_timestamp,
    _record_loaded_timestamp,
    _date_partition
from final
