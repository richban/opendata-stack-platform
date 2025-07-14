MODEL (
  name taxi.fact_taxi_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime
  ),
  partitioned_by [DATE_TRUNC('MONTH', pickup_datetime)],
  grain (trip_id, pickup_datetime),
  audits [
    ASSERT_UNIQUE_KEY(key_column := taxi_trip_key),
    ASSERT_UNIQUE_KEY(key_column := trip_id),
    ASSERT_NOT_NULL(column_name := taxi_type),
    ASSERT_NOT_NULL(column_name := fare_amount),
    ASSERT_NOT_NULL(column_name := total_amount),
    ASSERT_NOT_NULL(column_name := pickup_datetime),
    ASSERT_NOT_NULL(column_name := dropoff_datetime),
    assert_valid_trip_duration,
    assert_fhvhv_total_amount_consistency,
    ASSERT_NON_NEGATIVE_AMOUNT(amount_column := total_amount)
  ]
);

WITH validated_trips AS (
  SELECT
    taxi_type, /* Source identifier */
    trip_id,
    vendor_id, /* Simple direct references */
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
    rate_code_id, /* Dimension keys */
    payment_type_id,
    trip_type_id,
    _date_partition, /* Metadata */
    _incremental_timestamp,
    _record_loaded_timestamp,
    CAST(EXTRACT(YEAR FROM pickup_datetime) * 10000 + EXTRACT(MONTH FROM pickup_datetime) * 100 + EXTRACT(DAY FROM pickup_datetime) AS INT) AS date_key_pickup, /* Date and time fields for dimension lookups */ /* Format date_key as YYYYMMDD integer - match dim_date format */
    CAST(EXTRACT(YEAR FROM dropoff_datetime) * 10000 + EXTRACT(MONTH FROM dropoff_datetime) * 100 + EXTRACT(DAY FROM dropoff_datetime) AS INT) AS date_key_dropoff,
    CAST(DATE_PART('hour', pickup_datetime) * 3600 + DATE_PART('minute', pickup_datetime) * 60 AS INT) AS seconds_of_day_pickup, /* Create matching time values for joining to dim_time */ /* IMPORTANT: Round to minute precision (60 seconds) to match dim_time */
    CAST(DATE_PART('hour', dropoff_datetime) * 3600 + DATE_PART('minute', dropoff_datetime) * 60 AS INT) AS seconds_of_day_dropoff /* Calculate seconds of day from dropoff datetime (0-86399) */
  FROM taxi.silver_taxi_trips_validated
  WHERE
    pickup_datetime BETWEEN @start_ds AND @end_ds
), trips_with_time_keys /* Get time_key for pickup and dropoff */ AS (
  SELECT
    t.*,
    t_pickup.time_key AS time_key_pickup,
    t_dropoff.time_key AS time_key_dropoff
  FROM validated_trips AS t
  LEFT JOIN taxi.dim_time AS t_pickup
    ON t.seconds_of_day_pickup = t_pickup.seconds_of_day
  LEFT JOIN taxi.dim_time AS t_dropoff
    ON t.seconds_of_day_dropoff = t_dropoff.seconds_of_day
), final /* Final select with dimension key lookups */ AS (
  SELECT
    c.taxi_type,
    c.trip_id, /* natural key */
    c.date_key_pickup,
    c.date_key_dropoff,
    c.time_key_pickup,
    c.time_key_dropoff,
    COALESCE(v.vendor_key, -1) AS vendor_key, /* Foreign keys from dimensions */
    r.rate_code_key,
    p.payment_type_key,
    t.trip_type_key,
    COALESCE(pu_loc.location_key, 264) AS pu_location_key, /* Use coalesce to map invalid locations to unknown location (264) */
    COALESCE(do_loc.location_key, 264) AS do_location_key,
    c.passenger_count, /* Trip metrics */
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
    c.pickup_datetime, /* Timestamps */
    c.dropoff_datetime,
    c._incremental_timestamp, /* Metadata */
    c._record_loaded_timestamp,
    c._date_partition,
    MD5(CONCAT(c.taxi_type, '|', c.trip_id, '|', c.pickup_datetime::TEXT)) AS taxi_trip_key /* Generate a surrogate key for the fact table as a STRING type */ /* Use taxi_type, trip_id, and pickup time to ensure uniqueness */
  FROM trips_with_time_keys AS c
  LEFT JOIN taxi.dim_vendor AS v
    ON c.vendor_id = v.vendor_id
  LEFT JOIN taxi.dim_rate_code AS r
    ON c.rate_code_id = r.rate_code_id
  LEFT JOIN taxi.dim_payment_type AS p
    ON c.payment_type_id = p.payment_type_id
  LEFT JOIN taxi.dim_trip_type AS t
    ON c.trip_type_id = t.trip_type_id
  /* Join to location dimension, invalid locations will get NULL which we coalesce to 264 */
  LEFT JOIN taxi.dim_location AS pu_loc
    ON c.pu_location_id = pu_loc.location_id
  LEFT JOIN taxi.dim_location AS do_loc
    ON c.do_location_id = do_loc.location_id
)
SELECT
  taxi_trip_key,
  trip_id, /* natural key */
  taxi_type, /* Dimension keys */
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
  passenger_count, /* Trip metrics */
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
  pickup_datetime, /* Timestamps */
  dropoff_datetime,
  _incremental_timestamp, /* Metadata */
  _record_loaded_timestamp,
  _date_partition
FROM final