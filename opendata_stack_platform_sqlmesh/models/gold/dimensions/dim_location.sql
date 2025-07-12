MODEL (
  name taxi.dim_location,
  kind FULL
);

WITH final AS (
    SELECT
        geom_data,
        NULL AS valid_to,
        TRUE AS is_current,
        COALESCE(zone_id, 264) AS location_key,
        COALESCE(zone_id, 264) AS location_id,
        COALESCE(zone_name, 'Unknown Zone') AS location_name,
        COALESCE(borough_name, 'Unknown Borough') AS borough_name,
        COALESCE(area_size, 0.0) AS area_size,
        COALESCE(perimeter_length, 0.0) AS perimeter_length,
        CURRENT_TIMESTAMP AS valid_from
    FROM "nyc_database"."main"."taxi_zone_lookup"
    UNION ALL
    SELECT
        NULL AS geom_data,
        NULL AS valid_to,
        TRUE AS is_current,
        264 AS location_key,
        264 AS location_id,
        'Unknown Zone' AS location_name,
        'Unknown Borough' AS borough_name,
        0.0 AS area_size,
        0.0 AS perimeter_length,
        CURRENT_TIMESTAMP AS valid_from
)

SELECT * FROM final