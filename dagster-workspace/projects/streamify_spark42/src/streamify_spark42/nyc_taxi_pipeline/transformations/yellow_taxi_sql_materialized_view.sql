CREATE MATERIALIZED VIEW transformed_yellow_taxi AS
SELECT
    VendorID AS vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    ROUND((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0, 2) AS trip_duration_minutes,
    ROUND(fare_amount / NULLIF(trip_distance, 0), 2) AS fare_per_mile
FROM raw_yellow_taxi
WHERE trip_distance > 0 AND passenger_count > 0
