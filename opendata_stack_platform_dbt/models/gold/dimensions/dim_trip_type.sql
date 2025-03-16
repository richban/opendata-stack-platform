{{ config(
    materialized='incremental',
    unique_key='trip_type_key',
    incremental_strategy='delete+insert',
) }}

/*
This dimension categorizes whether a trip was a street-hail or dispatch.
Only used for Green Taxi and HVFHV trips.
Yellow Taxis are always street-hail by regulation.
*/

with trip_type_codes as (
    select distinct
        trip_type_id,
        case
            when trip_type_id = 1 then 'Street-hail'
            when trip_type_id = 2 then 'Dispatch'
            else 'Unknown (' || trip_type_id || ')'
        end as trip_type_desc
    from (
        -- Get all distinct trip_type values from Green Taxi (as int)
        -- FHVHV is always dispatch type (2)
        -- Yellow Taxi is always street-hail (1)
        select distinct trip_type as trip_type_id
        from {{ source('silver_green', 'green_taxi_trip') }}
        where trip_type is not null
    )
    union all
    -- FHVHV is always dispatch type (3)
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
