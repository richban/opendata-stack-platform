{{ config(
    materialized='table',
    schema='gold'
) }}

with time_sequence as (
    select generate_series as seconds_of_day
    from (
        select *
        from generate_series(0, 86399, 60)  -- Every minute (60 seconds)
    )
),

time_attributes as (
    select
        seconds_of_day,

        -- Create proper time_key in standard format (HHMMSS)
        cast(
            (seconds_of_day / 3600) * 10000
            + ((seconds_of_day % 3600) / 60) * 100
            + (seconds_of_day % 60)
            as int
        ) as time_key,

        -- Time components as proper integers
        cast(seconds_of_day / 3600 as int) as hour_24,
        cast((seconds_of_day % 3600) / 60 as int) as minute_value,

        cast(seconds_of_day % 60 as int) as second_value,
        cast(
            ((seconds_of_day / 3600) between 7 and 9)
            or ((seconds_of_day / 3600) between 16 and 19)
            as boolean
        ) as is_rush_hour,

        -- Time period classifications
        cast(
            (seconds_of_day / 3600) between 7 and 20
            as boolean
        ) as is_peak_time,

        -- AM/PM indicator
        case
            when cast(seconds_of_day / 3600 as int) = 0 then 12
            when cast(seconds_of_day / 3600 as int) > 12 then cast(seconds_of_day / 3600 as int) - 12
            else cast(seconds_of_day / 3600 as int)
        end as hour_12,

        -- Rush hour flag
        case
            when (seconds_of_day / 3600) between 6 and 9 then 'Morning Rush'
            when (seconds_of_day / 3600) between 10 and 15 then 'Midday'
            when (seconds_of_day / 3600) between 16 and 19 then 'Evening Rush'
            when (seconds_of_day / 3600) between 20 and 23 then 'Evening'
            else 'Late Night/Early Morning'
        end as period_of_day,

        -- Peak time flag (broader than rush hour, includes lunch)
        case
            when (seconds_of_day / 3600) < 12 then 'AM'
            else 'PM'
        end as am_pm_flag
    from time_sequence
)

select * from time_attributes
order by seconds_of_day
