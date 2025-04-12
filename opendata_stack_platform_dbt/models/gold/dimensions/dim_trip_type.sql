{{ config(
    materialized='table',
    unique_key='trip_type_key',
) }}

/*
This dimension categorizes whether a trip was a street-hail or dispatch.
Only used for Green Taxi and HVFHV trips.
Yellow Taxis are always street-hail by regulation.
*/

with trip_type_codes as (
    select
        1 as trip_type_id,
        'Street-hail' as trip_type_desc
    union all
    select
        2 as trip_type_id,
        'Dispatch' as trip_type_desc
    union all
    select
        3 as trip_type_id,
        'e-Dispatch' as trip_type_desc
),

final as (
    select
        trip_type_id as trip_type_key,
        trip_type_id,
        trip_type_desc,
        null::timestamp as valid_to,
        true as is_current,
        current_timestamp as valid_from
    from trip_type_codes
)

select * from final
