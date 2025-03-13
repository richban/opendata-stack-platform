{{ config(
    materialized='table',
    schema='gold'
) }}

with trip_types as (
    select
        1 as trip_type_id,
        'Street-hail' as trip_type_desc
    union all
    select
        2 as trip_type_id,
        'Dispatch' as trip_type_desc
    union all
    select
        0 as trip_type_id,
        'Unknown' as trip_type_desc
),

final as (
    select
        trip_type_id,
        trip_type_desc,
        cast(null as date) as row_expiration_date,
        'Y' as current_flag,
        row_number() over (
            order by trip_type_id
        ) as trip_type_key,
        current_date as row_effective_date
    from trip_types
)

select * from final
