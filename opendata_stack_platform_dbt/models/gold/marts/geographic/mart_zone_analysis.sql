{{ config(
    materialized='table',
    schema='gold'
) }}

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

with zone_metrics as (
    select
        -- define the granularity
        l_pu.location_id as pickup_location_id,
        l_do.location_id as dropoff_location_id,
        d.year_number,
        d.month_name,
        d.day_name,
        t.period_of_day,
        f.taxi_type,

        -- Aggregate other descriptive/related fields
        MIN(l_pu.location_name) as pickup_zone,
        MIN(l_pu.borough_name) as pickup_borough,
        MIN(l_do.location_name) as dropoff_zone,
        MIN(l_do.borough_name) as dropoff_borough,

        -- Count of trips
        COUNT(*) as trip_count,

        -- Distance metrics
        AVG(f.trip_distance) as avg_distance,
        SUM(f.trip_distance) as total_distance,

        -- Duration metrics (in minutes)
        AVG(EXTRACT(epoch from (f.dropoff_datetime - f.pickup_datetime)) / 60) as avg_duration_minutes,

        -- Revenue metrics
        SUM(f.fare_amount) as total_fare,
        SUM(f.total_amount) as total_revenue,
        AVG(f.fare_amount) as avg_fare,
        AVG(f.total_amount) as avg_total,

        -- Passenger metrics
        SUM(f.passenger_count) as total_passengers,
        AVG(f.passenger_count) as avg_passengers_per_trip

    from {{ ref('fact_taxi_trip') }} f
    left join {{ ref('dim_location') }} l_pu
        on f.pu_location_key = l_pu.location_key
    left join {{ ref('dim_location') }} l_do
        on f.do_location_key = l_do.location_key
    left join {{ ref('dim_date') }} d
        on f.date_key_pickup = d.date_key
    left join {{ ref('dim_time') }} t
        on f.time_key_pickup = t.time_key
    group by 1, 2, 3, 4, 5, 6, 7
),

-- Pickup zone summary
pickup_zone_summary as (
    select
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        year_number,
        month_name,
        day_name,
        period_of_day,
        taxi_type,
        SUM(trip_count) as total_pickups,
        SUM(total_revenue) as total_pickup_revenue,
        AVG(avg_distance) as avg_pickup_distance,
        AVG(avg_duration_minutes) as avg_pickup_duration,
        SUM(total_passengers) as total_pickup_passengers
    from zone_metrics
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

-- Dropoff zone summary
dropoff_zone_summary as (
    select
        dropoff_location_id,
        dropoff_zone,
        dropoff_borough,
        year_number,
        month_name,
        day_name,
        period_of_day,
        taxi_type,
        SUM(trip_count) as total_dropoffs,
        SUM(total_revenue) as total_dropoff_revenue,
        AVG(avg_distance) as avg_dropoff_distance,
        AVG(avg_duration_minutes) as avg_dropoff_duration,
        SUM(total_passengers) as total_dropoff_passengers
    from zone_metrics
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

-- Zone metrics with pickup and dropoff summaries
final as (
    select
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
        zm.trip_count as pair_trip_count,
        zm.total_revenue as pair_revenue,
        zm.avg_distance as pair_avg_distance,
        zm.avg_duration_minutes as pair_avg_duration,
        zm.total_passengers as pair_total_passengers,

        -- Percentage calculation
        zm.trip_count * 100.0 / SUM(zm.trip_count) over () as percentage_of_all_trips,

        -- All calculated metrics
        z.total_pickups / NULLIF(d.total_dropoffs, 0) as pickup_to_dropoff_ratio,
        z.total_pickup_revenue / NULLIF(z.total_pickups, 0) as revenue_per_pickup,
        case
            when z.avg_pickup_duration > 0
                then z.avg_pickup_distance / (z.avg_pickup_duration / 60)
            else null
        end as avg_pickup_speed_mph

    from zone_metrics zm
    inner join pickup_zone_summary z
        on
            zm.pickup_location_id = z.pickup_location_id
            and zm.year_number = z.year_number
            and zm.month_name = z.month_name
            and zm.day_name = z.day_name
            and zm.period_of_day = z.period_of_day
            and zm.taxi_type = z.taxi_type
    inner join dropoff_zone_summary d
        on
            zm.dropoff_location_id = d.dropoff_location_id
            and zm.year_number = d.year_number
            and zm.month_name = d.month_name
            and zm.day_name = d.day_name
            and zm.period_of_day = d.period_of_day
            and zm.taxi_type = d.taxi_type
)

select * from final
