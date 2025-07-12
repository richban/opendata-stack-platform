MODEL (
  name taxi.dim_vendor,
  kind FULL
);

/*
This dimension identifies taxi service providers and technology vendors.
Implemented as a hardcoded reference table for stability and business definition control.
*/

WITH vendor_reference AS (
    -- Static reference mapping that doesn't rely on source data
    -- Each vendor has a stable vendor_key for joining
    SELECT t.* FROM (
        VALUES
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
    ) AS t (vendor_key, vendor_type, vendor_id, vendor_name)
),

final AS (
    SELECT
        vendor_key,
        vendor_id,
        vendor_name,
        NULL AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP AS valid_from
    FROM vendor_reference
    ORDER BY vendor_key ASC
)

SELECT * FROM final