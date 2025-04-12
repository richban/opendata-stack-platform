{{ config(
    materialized='table',
    unique_key='rate_code_key',
) }}

/*
This dimension describes tariff types or negotiated rates used during taxi trips.
Rate codes vary by taxi type, with Yellow and Green taxis sharing a similar structure.
*/

with rate_code_mapping as (
    select
        1 as rate_code_id,
        'Standard rate' as rate_code_desc
    union all
    select
        2 as rate_code_id,
        'JFK' as rate_code_desc
    union all
    select
        3 as rate_code_id,
        'Newark' as rate_code_desc
    union all
    select
        4 as rate_code_id,
        'Nassau or Westchester' as rate_code_desc
    union all
    select
        5 as rate_code_id,
        'Negotiated fare' as rate_code_desc
    union all
    select
        6 as rate_code_id,
        'Group ride' as rate_code_desc
    union all
    select
        99 as rate_code_id,
        'Unknown' as rate_code_desc
),

final as (
    select
        rate_code_id as rate_code_key,
        rate_code_id,
        rate_code_desc,
        null as valid_to,
        true as is_current,
        current_timestamp as valid_from
    from rate_code_mapping
)

select * from final
