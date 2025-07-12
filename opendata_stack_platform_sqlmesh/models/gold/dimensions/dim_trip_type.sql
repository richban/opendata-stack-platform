MODEL (
  name taxi.dim_trip_type,
  kind FULL
);

/*
This dimension categorizes whether a trip was a street-hail or dispatch.
Only used for Green Taxi and HVFHV trips.
Yellow Taxis are always street-hail by regulation.
*/

WITH trip_type_codes AS (
    SELECT
        1 AS trip_type_id,
        'Street-hail' AS trip_type_desc
    UNION ALL
    SELECT
        2 AS trip_type_id,
        'Dispatch' AS trip_type_desc
    UNION ALL
    SELECT
        3 AS trip_type_id,
        'e-Dispatch' AS trip_type_desc
),

final AS (
    SELECT
        trip_type_id AS trip_type_key,
        trip_type_id,
        trip_type_desc,
        NULL::TIMESTAMP AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP AS valid_from
    FROM trip_type_codes
)

SELECT * FROM final