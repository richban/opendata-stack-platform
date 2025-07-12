MODEL (
  name taxi.dim_time,
  kind FULL
);

WITH time_sequence AS (
    SELECT * AS seconds_of_day
    FROM (
        SELECT *
        FROM generate_series(0, 86399, 60)  -- Every minute (60 seconds)
    )
),

time_attributes AS (
    SELECT
        seconds_of_day,

        -- Create proper time_key in standard format (HHMMSS)
        CAST(
            (seconds_of_day / 3600) * 10000
            + ((seconds_of_day % 3600) / 60) * 100
            + (seconds_of_day % 60)
            AS INT
        ) AS time_key,

        -- Time components as proper integers
        CAST(seconds_of_day / 3600 AS INT) AS hour_24,
        CAST((seconds_of_day % 3600) / 60 AS INT) AS minute_value,

        CAST(seconds_of_day % 60 AS INT) AS second_value,
        CAST(
            ((seconds_of_day / 3600) BETWEEN 7 AND 9)
            OR ((seconds_of_day / 3600) BETWEEN 16 AND 19)
            AS BOOLEAN
        ) AS is_rush_hour,

        -- Time period classifications
        CAST(
            (seconds_of_day / 3600) BETWEEN 7 AND 20
            AS BOOLEAN
        ) AS is_peak_time,

        -- AM/PM indicator
        CASE
            WHEN CAST(seconds_of_day / 3600 AS INT) = 0 THEN 12
            WHEN CAST(seconds_of_day / 3600 AS INT) > 12 THEN CAST(seconds_of_day / 3600 AS INT) - 12
            ELSE CAST(seconds_of_day / 3600 AS INT)
        END AS hour_12,

        -- Rush hour flag
        CASE
            WHEN (seconds_of_day / 3600) BETWEEN 6 AND 9 THEN 'Morning Rush'
            WHEN (seconds_of_day / 3600) BETWEEN 10 AND 15 THEN 'Midday'
            WHEN (seconds_of_day / 3600) BETWEEN 16 AND 19 THEN 'Evening Rush'
            WHEN (seconds_of_day / 3600) BETWEEN 20 AND 23 THEN 'Evening'
            ELSE 'Late Night/Early Morning'
        END AS period_of_day,

        -- Peak time flag (broader than rush hour, includes lunch)
        CASE
            WHEN (seconds_of_day / 3600) < 12 THEN 'AM'
            ELSE 'PM'
        END AS am_pm_flag
    FROM time_sequence
)

SELECT * FROM time_attributes
ORDER BY seconds_of_day