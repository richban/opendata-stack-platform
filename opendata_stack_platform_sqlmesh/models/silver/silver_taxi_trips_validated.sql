MODEL (
  name taxi.silver_taxi_trips_validated,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime
  ),
  start '2024-01-01',
  partitioned_by [date_trunc('month', pickup_datetime)],
  grain (trip_id, pickup_datetime)
);

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
    _date_partition,
    _incremental_timestamp,
    _record_loaded_timestamp,
    _is_valid
from taxi.silver_taxi_trips
where
    _is_valid = true
and pickup_datetime BETWEEN @start_ds AND @end_ds
