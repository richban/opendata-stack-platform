{{ config(
    materialized='table',
    schema='gold'
) }}

with rate_codes as (
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
        rate_code_id,
        rate_code_desc,
        cast(null as date) as row_expiration_date,
        'Y' as current_flag,
        row_number() over (
            order by rate_code_id
        ) as rate_code_key,
        current_date as row_effective_date
    from rate_codes
)

select * from final
