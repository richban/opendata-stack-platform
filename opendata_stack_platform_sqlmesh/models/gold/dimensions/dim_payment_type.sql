MODEL (
  name taxi.dim_payment_type,
  kind FULL
);

WITH payment_types AS (
    SELECT
        payment_type_id,
        CASE
            WHEN payment_type_id = 1 THEN 'Credit card'
            WHEN payment_type_id = 2 THEN 'Cash'
            WHEN payment_type_id = 3 THEN 'No charge'
            WHEN payment_type_id = 4 THEN 'Dispute'
            WHEN payment_type_id = 5 THEN 'Unknown'
            WHEN payment_type_id = 6 THEN 'Voided trip'
            ELSE 'Unknown Payment Type: ' || payment_type_id
        END AS payment_desc
    FROM (
        SELECT DISTINCT payment_type_id
        FROM (
            SELECT 1 AS payment_type_id
            UNION ALL
            SELECT 2
            UNION ALL
            SELECT 3
            UNION ALL
            SELECT 4
            UNION ALL
            SELECT 5
            UNION ALL
            SELECT 6
        )
    )
),

final AS (
    SELECT
        payment_type_id AS payment_type_key,
        payment_type_id,
        payment_desc,
        CAST(NULL AS DATE) AS row_expiration_date,
        'Y' AS current_flag,
        CURRENT_DATE AS row_effective_date
    FROM payment_types
)

SELECT * FROM final