{{ config(
    materialized='incremental',
    unique_key='rate_code_key',
    incremental_strategy='delete+insert',
) }}

/*
This dimension describes tariff types or negotiated rates used during taxi trips.
Rate codes vary by taxi type, with Yellow and Green taxis sharing a similar structure.
*/

with rate_code_mapping as (
    select
        rate_code_key,
        case
            when rate_code_key = 1 then 'Standard rate'
            when rate_code_key = 2 then 'JFK'
            when rate_code_key = 3 then 'Newark'
            when rate_code_key = 4 then 'Nassau or Westchester'
            when rate_code_key = 5 then 'Negotiated fare'
            when rate_code_key = 6 then 'Group ride'
            when rate_code_key = 99 then 'Unknown'
            else 'Unknown'
        end as rate_code_desc
    from (
        -- Get all distinct ratecode_id values from Yellow Taxi
        select distinct coalesce(cast(ratecode_id as int), 99) as rate_code_key
        from {{ source('silver_yellow', 'yellow_taxi_trip') }}

        union

        -- Get all distinct ratecode_id values from Green Taxi
        select distinct coalesce(cast(ratecode_id as int), 99) as rate_code_key
        from {{ source('silver_green', 'green_taxi_trip') }}
    )
),

final as (
    select
        rate_code_key,
        rate_code_key as rate_code_id,
        rate_code_desc,
        null as valid_to,
        true as is_current,
        current_timestamp as valid_from
    from rate_code_mapping
)

select * from final
