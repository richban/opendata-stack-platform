/*
    Test: Verify sum of pair trips equals total pickups

    This test ensures that for each pickup location and time period,
    the sum of all pair_trip_count values (representing trips to specific destinations)
    matches the total_pickups value (representing all trips from that origin).

    If this test fails, it indicates an inconsistency in how trips are being counted
    between the pair-level and the zone-level aggregations.
*/

select
    pickup_location_id,
    year_number,
    month_name,
    day_name,
    period_of_day,
    taxi_type,
    total_pickups,
    SUM(pair_trip_count) as sum_of_pair_trips,
    ABS(total_pickups - SUM(pair_trip_count)) as difference
from {{ ref('mart_zone_analysis') }}
group by 1, 2, 3, 4, 5, 6, 7
having ABS(total_pickups - SUM(pair_trip_count)) > 0.01
