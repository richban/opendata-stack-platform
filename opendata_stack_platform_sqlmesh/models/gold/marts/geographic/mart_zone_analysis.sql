MODEL (
  name ducklake.analytics.mart_zone_analysis,
  kind FULL
);

/*
    Mart: Zone Analysis

    Purpose: Provides detailed geographic analysis of taxi trip patterns across NYC zones,
    enabling insights into demand distribution, service coverage, and zone-specific metrics.

    This mart answers key business questions such as:
    - Which zones generate the most trips and revenue?
    - How do trip patterns differ across boroughs?
    - What are the most common pickup-dropoff zone pairs?
    - Which areas are underserved or overserved?

    Key metrics:
    - Trip counts and revenue by zone
    - Zone-to-zone trip patterns
    - Zone performance metrics (revenue per trip, avg distance)
    - Service coverage by time period
*/

WITH zone_metrics AS (
    SELECT
        -- define the granularity
        l_pu.location_id AS pickup_location_id,
        l_do.location_id AS dropoff_location_id,
        d.year_number,
        d.month_name,
        d.day_name,
        t.period_of_day,
        f.taxi_type,

        -- Aggregate other descriptive/related fields
        MIN(l_pu.location_name) AS pickup_zone,
        MIN(l_pu.borough_name) AS pickup_borough,
        MIN(l_do.location_name) AS dropoff_zone,
        MIN(l_do.borough_name) AS dropoff_borough,

        -- Count of trips
        COUNT(*) AS trip_count,

        -- Distance metrics
        AVG(f.trip_distance) AS avg_distance,
        SUM(f.trip_distance) AS total_distance,

        -- Duration metrics (in minutes)
        AVG(EXTRACT(epoch FROM (f.dropoff_datetime - f.pickup_datetime)) / 60) AS avg_duration_minutes,

        -- Revenue metrics
        SUM(f.fare_amount) AS total_fare,
        SUM(f.total_amount) AS total_revenue,
        AVG(f.fare_amount) AS avg_fare,
        AVG(f.total_amount) AS avg_total,

        -- Passenger metrics
        SUM(f.passenger_count) AS total_passengers,
        AVG(f.passenger_count) AS avg_passengers_per_trip

    FROM taxi.fact_taxi_trip f
    LEFT JOIN taxi.dim_location l_pu
        ON f.pu_location_key = l_pu.location_key
    LEFT JOIN taxi.dim_location l_do
        ON f.do_location_key = l_do.location_key
    LEFT JOIN taxi.dim_date d
        ON f.date_key_pickup = d.date_key
    LEFT JOIN taxi.dim_time t
        ON f.time_key_pickup = t.time_key
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- Pickup zone summary
pickup_zone_summary AS (
    SELECT
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        year_number,
        month_name,
        day_name,
        period_of_day,
        taxi_type,
        SUM(trip_count) AS total_pickups,
        SUM(total_revenue) AS total_pickup_revenue,
        AVG(avg_distance) AS avg_pickup_distance,
        AVG(avg_duration_minutes) AS avg_pickup_duration,
        SUM(total_passengers) AS total_pickup_passengers
    FROM zone_metrics
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- Dropoff zone summary
dropoff_zone_summary AS (
    SELECT
        dropoff_location_id,
        dropoff_zone,
        dropoff_borough,
        year_number,
        month_name,
        day_name,
        period_of_day,
        taxi_type,
        SUM(trip_count) AS total_dropoffs,
        SUM(total_revenue) AS total_dropoff_revenue,
        AVG(avg_distance) AS avg_dropoff_distance,
        AVG(avg_duration_minutes) AS avg_dropoff_duration,
        SUM(total_passengers) AS total_dropoff_passengers
    FROM zone_metrics
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- Zone metrics with pickup and dropoff summaries
final AS (
    SELECT
        zm.pickup_location_id,
        zm.pickup_zone,
        zm.pickup_borough,
        zm.dropoff_location_id,
        zm.dropoff_zone,
        zm.dropoff_borough,
        zm.year_number,
        zm.month_name,
        zm.day_name,
        zm.period_of_day,
        zm.taxi_type,

        -- All pickup zone metrics summary
        z.total_pickups,
        z.total_pickup_revenue,
        z.avg_pickup_distance,
        z.avg_pickup_duration,
        z.total_pickup_passengers,

        -- All dropoff zone metrics summary
        d.total_dropoffs,
        d.total_dropoff_revenue,
        d.avg_dropoff_distance,
        d.avg_dropoff_duration,
        d.total_dropoff_passengers,

        -- Zone pair metrics directly from zone_metrics
        zm.trip_count AS pair_trip_count,
        zm.total_revenue AS pair_revenue,
        zm.avg_distance AS pair_avg_distance,
        zm.avg_duration_minutes AS pair_avg_duration,
        zm.total_passengers AS pair_total_passengers,

        -- Percentage calculation
        zm.trip_count * 100.0 / SUM(zm.trip_count) OVER () AS percentage_of_all_trips,

        -- All calculated metrics
        z.total_pickups / NULLIF(d.total_dropoffs, 0) AS pickup_to_dropoff_ratio,
        z.total_pickup_revenue / NULLIF(z.total_pickups, 0) AS revenue_per_pickup,
        CASE
            WHEN z.avg_pickup_duration > 0
                THEN z.avg_pickup_distance / (z.avg_pickup_duration / 60)
            ELSE NULL
        END AS avg_pickup_speed_mph

    FROM zone_metrics zm
    INNER JOIN pickup_zone_summary z
        ON
            zm.pickup_location_id = z.pickup_location_id
            AND zm.year_number = z.year_number
            AND zm.month_name = z.month_name
            AND zm.day_name = z.day_name
            AND zm.period_of_day = z.period_of_day
            AND zm.taxi_type = z.taxi_type
    INNER JOIN dropoff_zone_summary d
        ON
            zm.dropoff_location_id = d.dropoff_location_id
            AND zm.year_number = d.year_number
            AND zm.month_name = d.month_name
            AND zm.day_name = d.day_name
            AND zm.period_of_day = d.period_of_day
            AND zm.taxi_type = d.taxi_type
)

SELECT * FROM final
