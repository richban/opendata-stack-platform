{{ config(
    materialized='table',
    schema='gold'
) }}

with yellow_vendors as (
    select distinct vendor_id
    from {{ source('silver_yellow', 'yellow_taxi_trip') }}
),

green_vendors as (
    select distinct vendor_id
    from {{ source('silver_green', 'green_taxi_trip') }}
),

fhvhv_vendors as (
    select distinct hvfhs_license_num as vendor_id
    from {{ source('silver_fhvhv', 'fhvhv_taxi_trip') }}
),

all_vendors as (
    select vendor_id from yellow_vendors
    union
    select vendor_id from green_vendors
    union
    select vendor_id from fhvhv_vendors
),

vendor_mapping as (
    select
        vendor_id,
        case
            when vendor_id = '1' then 'Creative Mobile Technologies, LLC'
            when vendor_id = '2' then 'VeriFone Inc.'
            when vendor_id = 'HV0002' then 'Juno'
            when vendor_id = 'HV0003' then 'Uber'
            when vendor_id = 'HV0004' then 'Via'
            when vendor_id = 'HV0005' then 'Lyft'
            else 'Unknown'
        end as vendor_name
    from all_vendors
),

final as (
    select
        vendor_id,
        vendor_name,
        cast(null as date) as row_expiration_date,
        'Y' as current_flag,
        row_number() over (
            order by vendor_id
        ) as vendor_key,
        current_date as row_effective_date
    from vendor_mapping
)

select * from final
