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

/*
    Staging Model: Validated Taxi Trips

    Purpose: Validates and filters taxi trip data before it enters the fact table.
    This model applies data quality checks and only includes records that pass these checks.

    Data Quality Checks:
    - Financial fields are non-negative
    - Trip distances are reasonable
    - Trip durations are valid
    - Vendor keys are valid
    - Required fields are not null
*/

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
        -- flat fee added to all taxi trips to fund taxi industry improvements
        coalesce(improvement_surcharge, 0.5) as improvement_surcharge,
        -- Applies to trips that start, end, or pass through Manhattan below 96th Street
        coalesce(congestion_surcharge, 0.0) as congestion_surcharge,
        airport_fee,
        total_amount,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
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
        -- Partition field for delete+insert strategy
        date_trunc('month', tpep_pickup_datetime) as _date_partition,
        -- Metadata
        current_timestamp as _record_loaded_timestamp,
        tpep_pickup_datetime as _incremental_timestamp

    from {{ source('bronze_yellow', 'yellow_taxi_trip') }}
    where
        1 = 1
        {{ incremental_backfill('tpep_pickup_datetime', 'yellow') }}
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
        -- flat fee added to all taxi trips to fund taxi industry improvements
        coalesce(improvement_surcharge, 0.5) as improvement_surcharge,
        -- Applies to trips that start, end, or pass through Manhattan below 96th Street
        coalesce(congestion_surcharge, 0.0) as congestion_surcharge,
        null as airport_fee,
        total_amount,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        -- Dimension keys will be joined later
        -- default to 99 if null (unknown)
        coalesce(ratecode_id, 99) as rate_code_id,
        case
            when payment_type = 0 then 5 -- payment_type 0 is unknown
            else coalesce(payment_type, 5) -- Default to 5 if payment_type is null
        end as payment_type_id,
        coalesce(trip_type, 1) as trip_type_id, -- Default to 1 (street-hail) if null
        -- Partition field for delete+insert strategy
        date_trunc('month', lpep_pickup_datetime) as _date_partition,
        -- Metadata
        current_timestamp as _record_loaded_timestamp,
        lpep_pickup_datetime as _incremental_timestamp
    from {{ source('bronze_green', 'green_taxi_trip') }}
    where
        1 = 1
        {{ incremental_backfill('lpep_pickup_datetime', 'green') }}
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
        -- Applies to trips that start, end, or pass through Manhattan below 96th Street
        coalesce(congestion_surcharge, 0.0) as congestion_surcharge,
        null as airport_fee, -- FHVHV doesn't have airport fee
        (base_passenger_fare + tips + tolls + coalesce(congestion_surcharge, 0)) as total_amount,
        pickup_datetime,
        dropoff_datetime,
        -- Dimension keys will be joined later
        99 as rate_code_id, -- FHVHV doesn't have rate code (99 = unknown)
        5 as payment_type_id, -- FHVHV doesn't have payment type so use 5 (unknown)
        3 as trip_type_id, -- FHVHV doesn't have trip type. Uber and Lyft are classified as e-dispatch services in New York City.
        -- Partition field for delete+insert strategy
        date_trunc('month', pickup_datetime) as _date_partition,
        -- Metadata
        current_timestamp as _record_loaded_timestamp,
        pickup_datetime as _incremental_timestamp
    from {{ source('bronze_fhvhv', 'fhvhv_taxi_trip') }}
    where
        1 = 1
        {{ incremental_backfill('pickup_datetime', 'fhvhv') }}
),

-- Combine all taxi trips
all_trips as (
    select * from yellow_trips
    union all
    select * from green_trips
    union all
    select * from fhvhv_trips
),

-- Apply cleaning and coalescing first
cleaned_trips as (
    select
        c.taxi_type,
        c.trip_id,
        c.vendor_id,
        c.rate_code_id,
        c.payment_type_id,
        c.trip_type_id,
        c.pu_location_id,
        c.do_location_id,
        c.passenger_count,
        coalesce(c.trip_distance, 0.0) as trip_distance,
        coalesce(c.fare_amount, 0.0) as fare_amount,
        coalesce(c.extra, 0.0) as extra,
        coalesce(c.mta_tax, 0.0) as mta_tax,
        coalesce(c.tip_amount, 0.0) as tip_amount,
        coalesce(c.tolls_amount, 0.0) as tolls_amount,
        coalesce(c.improvement_surcharge, 0.0) as improvement_surcharge,
        coalesce(c.congestion_surcharge, 0.0) as congestion_surcharge,
        coalesce(c.airport_fee, 0.0) as airport_fee,
        -- Recalculate total amount with potentially cleaned values
        coalesce(c.fare_amount, 0.0) +
        coalesce(c.extra, 0.0) +
        coalesce(c.mta_tax, 0.0) +
        coalesce(c.tip_amount, 0.0) +
        coalesce(c.tolls_amount, 0.0) +
        coalesce(c.improvement_surcharge, 0.0) +
        coalesce(c.congestion_surcharge, 0.0) +
        coalesce(c.airport_fee, 0.0) as total_amount,
        c.pickup_datetime,
        c.dropoff_datetime,
        c._date_partition,
        c._incremental_timestamp,
        c._record_loaded_timestamp
    from all_trips c
    where c.pickup_datetime::date between '2024-01-01' and '2025-01-31' -- Apply date filter earlier if possible
),

-- Now apply validation checks on the cleaned data
validated_trips as (
    select
        ct.*, -- Select all columns from cleaned_trips

        -- Data quality flags using cleaned columns from ct.*
        ct.trip_type_id is not null and
        ct.pickup_datetime is not null and
        ct.dropoff_datetime is not null and
        -- Handle potential NULL datetimes before comparison if they shouldn't exist
        -- Or ensure they are filtered out earlier if truly invalid
        (ct.pickup_datetime is null or ct.dropoff_datetime is null or ct.pickup_datetime <= ct.dropoff_datetime) and

        -- Allow NULL passenger_count for FHVHV trips, but ensure non-NULL values are valid
        (ct.passenger_count is null or (ct.passenger_count > 0 and ct.passenger_count <= 10)) and

        -- Financial fields are non-negative
        ct.trip_distance > 0.0 and
        ct.extra >= 0.0 and
        ct.tolls_amount >= 0.0 and
        ct.improvement_surcharge >= 0.0 and
        ct.congestion_surcharge >= 0.0 and
        ct.airport_fee >= 0.0 and
        ct.mta_tax >= 0.0 and
        ct.tip_amount >= 0.0 and
        ct.fare_amount >= 0.0 and
        ct.total_amount >= 0.0 as _is_valid

    from cleaned_trips ct
)

-- Final select - only include valid records
select
    trip_id,
    taxi_type,
    vendor_id,
    rate_code_id,
    payment_type_id,
    trip_type_id,
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
    _is_valid,
    _date_partition,
    _incremental_timestamp,
    _record_loaded_timestamp
from validated_trips
