{{ config(
    materialized='table',
    unique_key='payment_type_key',
) }}

with payment_types as (
    select
        payment_type_id,
        case
            when payment_type_id = 1 then 'Credit card'
            when payment_type_id = 2 then 'Cash'
            when payment_type_id = 3 then 'No charge'
            when payment_type_id = 4 then 'Dispute'
            when payment_type_id = 5 then 'Unknown'
            when payment_type_id = 6 then 'Voided trip'
            else 'Unknown Payment Type: ' || payment_type_id
        end as payment_desc
    from (
        select distinct payment_type_id
        from (
            select 1 as payment_type_id
            union all
            select 2
            union all
            select 3
            union all
            select 4
            union all
            select 5
            union all
            select 6
        )
    )
),

final as (
    select
        payment_type_id as payment_type_key,
        payment_type_id,
        payment_desc,
        cast(null as date) as row_expiration_date,
        'Y' as current_flag,
        current_date as row_effective_date
    from payment_types
)

select * from final
