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
)

select * from source_data
