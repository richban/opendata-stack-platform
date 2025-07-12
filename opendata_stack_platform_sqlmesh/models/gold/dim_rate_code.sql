MODEL (
  name taxi.dim_rate_code,
  kind FULL
);

/*
This dimension describes tariff types or negotiated rates used during taxi trips.
Rate codes vary by taxi type, with Yellow and Green taxis sharing a similar structure.
*/

WITH rate_code_mapping AS (
    SELECT
        1 AS rate_code_id,
        'Standard rate' AS rate_code_desc
    UNION ALL
    SELECT
        2 AS rate_code_id,
        'JFK' AS rate_code_desc
    UNION ALL
    SELECT
        3 AS rate_code_id,
        'Newark' AS rate_code_desc
    UNION ALL
    SELECT
        4 AS rate_code_id,
        'Nassau or Westchester' AS rate_code_desc
    UNION ALL
    SELECT
        5 AS rate_code_id,
        'Negotiated fare' AS rate_code_desc
    UNION ALL
    SELECT
        6 AS rate_code_id,
        'Group ride' AS rate_code_desc
    UNION ALL
    SELECT
        99 AS rate_code_id,
        'Unknown' AS rate_code_desc
),

final AS (
    SELECT
        rate_code_id AS rate_code_key,
        rate_code_id,
        rate_code_desc,
        NULL AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP AS valid_from
    FROM rate_code_mapping
)

SELECT * FROM final