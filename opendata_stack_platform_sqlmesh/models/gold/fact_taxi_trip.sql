MODEL (
  name taxi.fact_taxi_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime
  ),
  partitioned_by [date_trunc('month', pickup_datetime)],
  grain (trip_id, pickup_datetime)
);

WITH validated_trips AS (
    SELECT
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
        CAST(
            EXTRACT(year FROM pickup_datetime) * 10000
            + EXTRACT(month FROM pickup_datetime) * 100
            + EXTRACT(day FROM pickup_datetime)
            AS INT
        ) AS date_key_pickup,
        CAST(
            EXTRACT(year FROM dropoff_datetime) * 10000
            + EXTRACT(month FROM dropoff_datetime) * 100
            + EXTRACT(day FROM dropoff_datetime)
            AS INT
        ) AS date_key_dropoff,

        -- Create matching time values for joining to dim_time
        -- IMPORTANT: Round to minute precision (60 seconds) to match dim_time
        CAST(
            date_part('hour', pickup_datetime) * 3600
            + date_part('minute', pickup_datetime) * 60
            AS INT
        ) AS seconds_of_day_pickup,

        -- Calculate seconds of day from dropoff datetime (0-86399)
        CAST(
            date_part('hour', dropoff_datetime) * 3600
            + date_part('minute', dropoff_datetime) * 60
            AS INT
        ) AS seconds_of_day_dropoff
    FROM taxi.silver_taxi_trips_validated
    WHERE pickup_datetime BETWEEN @start_ds AND @end_ds
),

-- Get time_key for pickup and dropoff
trips_with_time_keys AS (
    SELECT
        t.*,
        t_pickup.time_key AS time_key_pickup,
        t_dropoff.time_key AS time_key_dropoff
    FROM validated_trips t
    LEFT JOIN taxi.dim_time t_pickup
        ON t.seconds_of_day_pickup = t_pickup.seconds_of_day
    LEFT JOIN taxi.dim_time t_dropoff
        ON t.seconds_of_day_dropoff = t_dropoff.seconds_of_day
),

-- Final select with dimension key lookups
final AS (
    SELECT
        c.taxi_type,
        c.trip_id, -- natural key
        c.date_key_pickup,
        c.date_key_dropoff,
        c.time_key_pickup,
        c.time_key_dropoff,

        -- Foreign keys from dimensions
        COALESCE(v.vendor_key, -1) AS vendor_key,
        r.rate_code_key,
        p.payment_type_key,
        t.trip_type_key,

        -- Use coalesce to map invalid locations to unknown location (264)
        COALESCE(pu_loc.location_key, 264) AS pu_location_key,
        COALESCE(do_loc.location_key, 264) AS do_location_key,

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
        MD5(CONCAT(c.taxi_type, '|', c.trip_id, '|', c.pickup_datetime::TEXT)) AS taxi_trip_key
    FROM trips_with_time_keys c
    LEFT JOIN taxi.dim_vendor v
        ON c.vendor_id = v.vendor_id
    LEFT JOIN taxi.dim_rate_code r
        ON c.rate_code_id = r.rate_code_id
    LEFT JOIN taxi.dim_payment_type p
        ON c.payment_type_id = p.payment_type_id
    LEFT JOIN taxi.dim_trip_type t
        ON c.trip_type_id = t.trip_type_id
    -- Join to location dimension, invalid locations will get NULL which we coalesce to 264
    LEFT JOIN taxi.dim_location pu_loc
        ON c.pu_location_id = pu_loc.location_id
    LEFT JOIN taxi.dim_location do_loc
        ON c.do_location_id = do_loc.location_id
)

SELECT
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
FROM final