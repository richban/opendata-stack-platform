{{ config(
    materialized='incremental',
    unique_key='location_key',
    incremental_strategy='delete+insert',
) }}

with final as (
    select
        geom_data,
        null as valid_to,
        true as is_current,
        coalesce(zone_id, 264) as location_key,
        coalesce(zone_id, 264) as location_id,
        coalesce(zone_name, 'Unknown Zone') as location_name,
        coalesce(borough_name, 'Unknown Borough') as borough_name,
        coalesce(area_size, 0.0) as area_size,
        coalesce(perimeter_length, 0.0) as perimeter_length,
        current_timestamp as valid_from
    from {{ source('taxi_zone_lookup', 'taxi_zone_lookup') }}
)

select * from final
