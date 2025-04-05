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
    store_and_fwd_flag,
    pickup_datetime,
    dropoff_datetime,
    _date_partition,
    _incremental_timestamp,
    _record_loaded_timestamp
from {{ ref('silver_taxi_trips') }}
where
    is_valid = true
    {{ incremental_backfill('pickup_datetime') }}
