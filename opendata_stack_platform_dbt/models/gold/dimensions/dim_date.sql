{{ config(
    materialized='table',
    schema=var('gold_schema', 'gold'),
    full_refresh=true
) }}

-- Generate date dimension for 10 years (2015-2025)
with date_spine as (
    select 
        date_add(DATE '2015-01-01', n * interval '1 day') as date_day
    from 
        (select row_number() over () - 1 as n from range(4018))
),

transformed as (
    select
        -- Primary key
        date_day as full_date,

        -- Date attributes
        (
            extract('year' from date_day) * 10000
            + extract('month' from date_day) * 100
            + extract('day' from date_day)
        )::int as date_key,
        case extract('dow' from date_day)::int
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        extract('dow' from date_day) as day_of_week,
        extract('day' from date_day) as day_of_month,
        extract('week' from date_day) as week_of_year,
        extract('month' from date_day) as month_number,
        case extract('month' from date_day)::int
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,
        extract('quarter' from date_day) as quarter_number,
        extract('year' from date_day) as year_number
    from date_spine
)

select * from transformed
