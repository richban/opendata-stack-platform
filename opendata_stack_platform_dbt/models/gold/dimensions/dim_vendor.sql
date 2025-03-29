{{ config(
    materialized='table',
    unique_key='vendor_key',
) }}

/*
This dimension identifies taxi service providers and technology vendors.
Implemented as a hardcoded reference table for stability and business definition control.
*/

with vendor_reference as (
    -- Static reference mapping that doesn't rely on source data
    -- Each vendor has a stable vendor_key for joining
    select * from (
        values
        -- Yellow/Green taxi vendors
        (1, 'yellow_green', 1, 'Creative Mobile Technologies, LLC'),
        (2, 'yellow_green', 2, 'VeriFone'),
        (6, 'yellow_green', 6, 'Myle Technologies Inc'),
        (7, 'yellow_green', 7, 'Helix'),

        -- FHVHV vendors - with stable surrogate keys
        (3, 'fhvhv', 3, 'Uber'),
        (4, 'fhvhv', 4, 'Via'),
        (5, 'fhvhv', 5, 'Lyft'),

        -- Default unknown vendor
        (-1, 'unknown', -1, 'Unknown Vendor')
    )
),

final as (
    select
        vendor_key,
        vendor_id,
        vendor_name,
        null as valid_to,
        true as is_current,
        CURRENT_TIMESTAMP as valid_from
    from vendor_reference
    order by vendor_key asc
)

select * from final
