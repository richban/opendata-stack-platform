{{ config(
    materialized='table',
    unique_key='vendor_key',
) }}

/*
This dimension identifies taxi service providers and technology vendors.
*/

with vendor_ids as (
    -- Yellow taxi vendor_ids
    select distinct vendor_id
    from {{ source('silver_yellow', 'yellow_taxi_trip') }}

    union all

    -- Green taxi vendor_ids
    select distinct vendor_id
    from {{ source('silver_green', 'green_taxi_trip') }}

    union all

    -- FHVHV (uber, lyft, etc) vendor_ids (hvfhs_license_num)
    select distinct
        -- Extract just the numeric part after 'HV'
        case
            when hvfhs_license_num like 'HV%'
                then
                    CAST(REPLACE(hvfhs_license_num, 'HV', '') as INT)
            else -1 -- Default for unknown pattern
        end as vendor_id
    from {{ source('silver_fhvhv', 'fhvhv_taxi_trip') }}
),

vendor_codes as (
    select distinct
        vendor_id,
        case
            -- Yellow and Green Taxi vendors
            when vendor_id = 1 then 'Creative Mobile Technologies'
            when vendor_id = 2 then 'VeriFone'
            -- FHVHV vendors
            when vendor_id = 2 then 'Juno'
            when vendor_id = 3 then 'Uber'
            when vendor_id = 4 then 'Via'
            when vendor_id = 5 then 'Lyft'
            else 'Unknown Vendor Code: ' || vendor_id
        end as vendor_name
    from vendor_ids
),

final as (
    select
        vendor_id as vendor_key,
        vendor_id,
        vendor_name,
        null::TIMESTAMP as valid_to,
        true as is_current,
        CURRENT_TIMESTAMP as valid_from
    from vendor_codes
)

select * from final
