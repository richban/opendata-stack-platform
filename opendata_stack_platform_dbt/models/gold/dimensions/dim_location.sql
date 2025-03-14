{{ config(
    materialized='table',
    schema='gold'
) }}

with source_data as (
    select
        -- Use the location_id as the surrogate key for simplicity
        -- In a real implementation, you might use an identity column or a sequence
        cast(zone_id as bigint) as location_key,
        zone_id as location_id,
        zone_name as location_name,
        geom_data,
        area_size,
        perimeter_length
    from {{ source('taxi_zone_lookup', 'taxi_zone_lookup') }}
),

-- Add unknown location record
unknown_location as (
    select
        'Unknown Location' as location_name,
        null as geom_data,
        0 as area_size,
        0 as perimeter_length,
        -1 as location_key,
        -1 as location_id
)

-- Combine valid locations with unknown location
select * from source_data
union all
select * from unknown_location
