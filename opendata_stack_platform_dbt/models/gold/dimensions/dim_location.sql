{{ config(
    materialized='incremental',
    unique_key='location_key',
    incremental_strategy='delete+insert',
) }}

/*
This dimension captures geographic information for pickup and dropoff locations.
It integrates taxi zone data with location IDs from taxi trips to support spatial analysis.
*/

with pickup_zones as (
    select distinct pu_location_id as location_id
    from {{ source('silver_yellow', 'yellow_taxi_trip') }}
    where pu_location_id is not null

    union

    select distinct pu_location_id as location_id
    from {{ source('silver_green', 'green_taxi_trip') }}
    where pu_location_id is not null

    union

    select distinct pu_location_id as location_id
    from {{ source('silver_fhvhv', 'fhvhv_taxi_trip') }}
    where pu_location_id is not null
),

dropoff_zones as (
    select distinct do_location_id as location_id
    from {{ source('silver_yellow', 'yellow_taxi_trip') }}
    where do_location_id is not null

    union

    select distinct do_location_id as location_id
    from {{ source('silver_green', 'green_taxi_trip') }}
    where do_location_id is not null

    union

    select distinct do_location_id as location_id
    from {{ source('silver_fhvhv', 'fhvhv_taxi_trip') }}
    where do_location_id is not null
),

all_zones as (
    select location_id from pickup_zones
    union
    select location_id from dropoff_zones
),

zone_info as (
    select distinct
        tz.zone_id as location_id,
        tz.zone_name,
        tz.borough_name,
        tz.geom_data,
        tz.area_size,
        tz.perimeter_length
    from {{ source('taxi_zone_lookup', 'taxi_zone_lookup') }} as tz
    right join all_zones as az on tz.zone_id = az.location_id
),

final as (
    select
        geom_data,
        null::timestamp as valid_to,
        true as is_current,
        coalesce(location_id, -1) as location_key,
        coalesce(location_id, -1) as location_id,
        coalesce(zone_name, 'Unknown Zone') as location_name,
        coalesce(borough_name, 'Unknown Borough') as borough_name,
        coalesce(area_size, 0.0) as area_size,
        coalesce(perimeter_length, 0.0) as perimeter_length,
        current_timestamp as valid_from
    from zone_info
)

select * from final
