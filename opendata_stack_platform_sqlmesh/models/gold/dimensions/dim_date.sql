MODEL (
  name taxi.dim_date,
  kind FULL
);

-- Generate date dimension for 10 years (2020-2035)
WITH date_spine AS (
    SELECT (DATE '2020-01-01' + INTERVAL (n) DAY) AS date_day
    FROM
        (SELECT row_number() OVER () - 1 AS n FROM range(4018))
),

transformed AS (
    SELECT
        -- Primary key
        date_day AS full_date,

        -- Date attributes
        (
            EXTRACT('year' FROM date_day) * 10000
            + EXTRACT('month' FROM date_day) * 100
            + EXTRACT('day' FROM date_day)
        )::INT AS date_key,
        CASE EXTRACT('dow' FROM date_day)::INT
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        EXTRACT('dow' FROM date_day) AS day_of_week,
        EXTRACT('day' FROM date_day) AS day_of_month,
        EXTRACT('week' FROM date_day) AS week_of_year,
        EXTRACT('month' FROM date_day) AS month_number,
        CASE EXTRACT('month' FROM date_day)::INT
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        EXTRACT('quarter' FROM date_day) AS quarter_number,
        EXTRACT('year' FROM date_day) AS year_number
    FROM date_spine
)

SELECT * FROM transformed