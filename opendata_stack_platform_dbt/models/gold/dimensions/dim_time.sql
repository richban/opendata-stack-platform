{{ config(
    materialized='table',
    schema='gold'
) }}

with time_sequence as (
    select generate_series as seconds_of_day
    from (
        select *
        from generate_series(0, 86399, 60)
    )
),

time_attributes as (
    select
        seconds_of_day,
        -- Create time key in format HHMMSS
        cast(
            (seconds_of_day / 3600) * 10000
            + ((seconds_of_day % 3600) / 60) * 100
            + (seconds_of_day % 60)
            as int
        ) as time_key,

        -- Time components
        (seconds_of_day / 3600) as hour_24,
        case
            when (seconds_of_day / 3600) = 0 then 12
            when (seconds_of_day / 3600) > 12 then (seconds_of_day / 3600) - 12
            else (seconds_of_day / 3600)
        end as hour_12,

        (seconds_of_day % 3600) / 60 as minute_value,
        seconds_of_day % 60 as second_value,

        -- Time period classifications
        case
            when (seconds_of_day / 3600) between 6 and 9 then 'Morning Rush'
            when (seconds_of_day / 3600) between 10 and 15 then 'Midday'
            when (seconds_of_day / 3600) between 16 and 19 then 'Evening Rush'
            when (seconds_of_day / 3600) between 20 and 23 then 'Evening'
            else 'Late Night/Early Morning'
        end as period_of_day,

        -- AM/PM indicator
        case
            when (seconds_of_day / 3600) < 12 then 'AM'
            else 'PM'
        end as am_pm_flag,

        -- Rush hour flag
        coalesce(
            ((seconds_of_day / 3600) between 7 and 9)
            or ((seconds_of_day / 3600) between 16 and 19),
            false
        ) as is_rush_hour,

        -- Peak time flag (broader than rush hour, includes lunch)
        coalesce(
            (seconds_of_day / 3600) between 7 and 20,
            false
        ) as is_peak_time
    from time_sequence
)

select * from time_attributes
order by seconds_of_day
