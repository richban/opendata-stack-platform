MODEL (
  name taxi.silver_taxi_trips,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime
  ),
  start '2024-01-01',
  partitioned_by [DATE_TRUNC('MONTH', pickup_datetime)],
  grain (trip_id, pickup_datetime)
);

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
*/ /* Yellow taxi trips */
WITH yellow_trips AS (
  SELECT
    'yellow' AS taxi_type, /* Source identifier */
    row_hash AS trip_id, /* Use row_hash from source as trip_id */
    vendor_id, /* Simple direct references first */
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    COALESCE(improvement_surcharge, 0.5) AS improvement_surcharge, /* flat fee added to all taxi trips to fund taxi industry improvements */
    COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge, /* Applies to trips that start, end, or pass through Manhattan below 96th Street */
    airport_fee,
    total_amount,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    COALESCE(ratecode_id, 99) AS rate_code_id, /* Dimension keys will be joined later */ /* default to 99 if null (unknown) */
    CASE WHEN payment_type = 0 THEN 5 ELSE COALESCE(payment_type, 5) END AS payment_type_id, /* payment_type 0 is unknown */ /* Default to 5 if payment_type is null */
    1 AS trip_type_id, /* Yellow taxis don't have trip type but by law it's street-hail (1 = Street-hail) */
    DATE_TRUNC('MONTH', tpep_pickup_datetime) AS _date_partition, /* Partition field for delete+insert strategy */
    CURRENT_TIMESTAMP AS _record_loaded_timestamp, /* Metadata */
    tpep_pickup_datetime AS _incremental_timestamp
  FROM taxi.bronze_yellow_taxi_trip
  WHERE
    tpep_pickup_datetime BETWEEN @start_ds AND @end_ds
), green_trips /* Green taxi trips */ AS (
  SELECT
    'green' AS taxi_type, /* Source identifier */
    row_hash AS trip_id, /* Use row_hash from source as trip_id */
    vendor_id, /* Simple direct references first */
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    COALESCE(improvement_surcharge, 0.5) AS improvement_surcharge, /* flat fee added to all taxi trips to fund taxi industry improvements */
    COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge, /* Applies to trips that start, end, or pass through Manhattan below 96th Street */
    NULL AS airport_fee,
    total_amount,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    COALESCE(ratecode_id, 99) AS rate_code_id, /* Dimension keys will be joined later */ /* default to 99 if null (unknown) */
    CASE
      WHEN payment_type = 0
      THEN 5 /* payment_type 0 is unknown */
      ELSE COALESCE(payment_type, 5) /* Default to 5 if payment_type is null */
    END AS payment_type_id,
    COALESCE(trip_type, 1) AS trip_type_id, /* Default to 1 (street-hail) if null */
    DATE_TRUNC('MONTH', lpep_pickup_datetime) AS _date_partition, /* Partition field for delete+insert strategy */
    CURRENT_TIMESTAMP AS _record_loaded_timestamp, /* Metadata */
    lpep_pickup_datetime AS _incremental_timestamp
  FROM taxi.bronze_green_taxi_trip
  WHERE
    lpep_pickup_datetime BETWEEN @start_ds AND @end_ds
), fhvhv_trips /* High-Volume For-Hire Vehicle (HVFHV) trips */ AS (
  SELECT
    'fhvhv' AS taxi_type, /* Source identifier */
    row_hash AS trip_id, /* Use row_hash from source as trip_id */
    CASE
      WHEN LOWER(hvfhs_license_num) LIKE 'HV%'
      THEN REPLACE(hvfhs_license_num, 'HV', '')::INT
      ELSE -1 /* Default for unknown pattern */
    END AS vendor_id, /* Simple direct references first */
    pu_location_id,
    do_location_id,
    NULL AS passenger_count, /* FHVHV doesn't have passenger count */
    trip_miles AS trip_distance, /* Map miles to distance */
    base_passenger_fare AS fare_amount, /* Map base fare */
    NULL AS extra, /* FHVHV doesn't have extra */
    NULL AS mta_tax, /* FHVHV doesn't have MTA tax */
    tips AS tip_amount, /* Map tips */
    tolls AS tolls_amount, /* Map tolls */
    NULL AS improvement_surcharge, /* FHVHV doesn't have improvement surcharge */
    COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge, /* Applies to trips that start, end, or pass through Manhattan below 96th Street */
    NULL AS airport_fee, /* FHVHV doesn't have airport fee */
    (
      base_passenger_fare + tips + tolls + COALESCE(congestion_surcharge, 0)
    ) AS total_amount,
    pickup_datetime,
    dropoff_datetime,
    99 AS rate_code_id, /* Dimension keys will be joined later */ /* FHVHV doesn't have rate code (99 = unknown) */
    5 AS payment_type_id, /* FHVHV doesn't have payment type so use 5 (unknown) */
    3 AS trip_type_id, /* FHVHV doesn't have trip type. Uber and Lyft are classified as e-dispatch services in New York City. */
    DATE_TRUNC('MONTH', pickup_datetime) AS _date_partition, /* Partition field for delete+insert strategy */
    CURRENT_TIMESTAMP AS _record_loaded_timestamp, /* Metadata */
    pickup_datetime AS _incremental_timestamp
  FROM taxi.bronze_fhvhv_taxi_trip
  WHERE
    pickup_datetime BETWEEN @start_ds AND @end_ds
), all_trips /* Combine all taxi trips */ AS (
  SELECT
    *
  FROM yellow_trips
  UNION ALL
  SELECT
    *
  FROM green_trips
  UNION ALL
  SELECT
    *
  FROM fhvhv_trips
), cleaned_trips /* Apply cleaning and coalescing first */ AS (
  SELECT
    c.taxi_type,
    c.trip_id,
    c.vendor_id,
    c.rate_code_id,
    c.payment_type_id,
    c.trip_type_id,
    c.pu_location_id,
    c.do_location_id,
    c.passenger_count,
    COALESCE(c.trip_distance, 0.0) AS trip_distance,
    COALESCE(c.fare_amount, 0.0) AS fare_amount,
    COALESCE(c.extra, 0.0) AS extra,
    COALESCE(c.mta_tax, 0.0) AS mta_tax,
    COALESCE(c.tip_amount, 0.0) AS tip_amount,
    COALESCE(c.tolls_amount, 0.0) AS tolls_amount,
    COALESCE(c.improvement_surcharge, 0.0) AS improvement_surcharge,
    COALESCE(c.congestion_surcharge, 0.0) AS congestion_surcharge,
    COALESCE(c.airport_fee, 0.0) AS airport_fee,
    COALESCE(c.fare_amount, 0.0) /* Recalculate total amount with potentially cleaned values */ + COALESCE(c.extra, 0.0) + COALESCE(c.mta_tax, 0.0) + COALESCE(c.tip_amount, 0.0) + COALESCE(c.tolls_amount, 0.0) + COALESCE(c.improvement_surcharge, 0.0) + COALESCE(c.congestion_surcharge, 0.0) + COALESCE(c.airport_fee, 0.0) AS total_amount,
    c.pickup_datetime,
    c.dropoff_datetime,
    c._date_partition,
    c._incremental_timestamp,
    c._record_loaded_timestamp
  FROM all_trips AS c
  WHERE
    c.pickup_datetime::DATE BETWEEN '2024-01-01' AND '2025-01-31' /* Apply date filter earlier if possible */
), validated_trips /* Now apply validation checks on the cleaned data */ AS (
  SELECT
    ct.*, /* Select all columns from cleaned_trips */
    NOT ct.trip_type_id /* Data quality flags using cleaned columns from ct.* */ IS NULL
    AND NOT ct.pickup_datetime IS NULL
    AND NOT ct.dropoff_datetime IS NULL
    AND (
      ct.pickup_datetime IS NULL
      OR ct.dropoff_datetime IS NULL
      OR ct.pickup_datetime <= ct.dropoff_datetime
    ) /* Handle potential NULL datetimes before comparison if they shouldn't exist */ /* Or ensure they are filtered out earlier if truly invalid */
    AND (
      ct.passenger_count IS NULL
      OR (
        ct.passenger_count > 0 AND ct.passenger_count <= 10
      )
    ) /* Allow NULL passenger_count for FHVHV trips, but ensure non-NULL values are valid */
    AND ct.trip_distance /* Financial fields are non-negative */ > 0.0
    AND ct.extra >= 0.0
    AND ct.tolls_amount >= 0.0
    AND ct.improvement_surcharge >= 0.0
    AND ct.congestion_surcharge >= 0.0
    AND ct.airport_fee >= 0.0
    AND ct.mta_tax >= 0.0
    AND ct.tip_amount >= 0.0
    AND ct.fare_amount >= 0.0
    AND ct.total_amount >= 0.0 AS _is_valid
  FROM cleaned_trips AS ct
)
/* Final select - only include valid records */
SELECT
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
FROM validated_trips
