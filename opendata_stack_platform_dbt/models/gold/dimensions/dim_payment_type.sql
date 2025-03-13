{{ config(
    materialized='table',
    schema='gold'
) }}

with payment_types as (
    select
        1 as payment_type_id,
        'Credit card' as payment_desc
    union all
    select
        2 as payment_type_id,
        'Cash' as payment_desc
    union all
    select
        3 as payment_type_id,
        'No charge' as payment_desc
    union all
    select
        4 as payment_type_id,
        'Dispute' as payment_desc
    union all
    select
        5 as payment_type_id,
        'Unknown' as payment_desc
    union all
    select
        6 as payment_type_id,
        'Voided trip' as payment_desc
),

final as (
    select
        payment_type_id,
        payment_desc,
        cast(null as date) as row_expiration_date,
        'Y' as current_flag,
        row_number() over (
            order by payment_type_id
        ) as payment_type_key,
        current_date as row_effective_date
    from payment_types
)

select * from final
