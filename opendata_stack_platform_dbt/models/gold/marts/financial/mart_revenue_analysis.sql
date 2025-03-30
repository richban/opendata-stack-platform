{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    Mart: Revenue Analysis

    Purpose: Provides a comprehensive view of all revenue metrics
    aggregated by various dimensions for financial analysis and reporting.

    This mart answers key business questions such as:
    - What are the total revenue trends over time?
    - How does revenue break down by vendor, borough, payment type?
    - What's the contribution of different fee types to overall revenue?
    - How do surge pricing and peak hours affect revenue?

    Key metrics:
    - Total fare revenue
    - Average fare per trip
    - Percentage contribution of surcharges and fees
    - Payment type distribution
    - Peak vs. off-peak comparisons
    - Time period revenue distribution
*/

with revenue_by_day as (
    select
        d.full_date as trip_date,
        d.day_name,
        t.period_of_day,
        t.is_rush_hour,
        d.month_name,
        d.quarter_number,
        d.year_number,
        f.taxi_type,
        v.vendor_name,
        l_pu.borough_name as pickup_borough,
        l_do.borough_name as dropoff_borough,
        pt.payment_desc,

        -- Revenue metrics
        count(*) as total_trips,
        sum(f.fare_amount) as total_fare_amount,
        sum(f.tip_amount) as total_tip_amount,
        sum(f.total_amount) as total_revenue,
        sum(f.extra) as total_extra_charges,
        sum(f.mta_tax) as total_mta_tax,
        sum(f.tolls_amount) as total_tolls,
        sum(f.improvement_surcharge) as total_improvement_surcharge,
        sum(f.congestion_surcharge) as total_congestion_surcharge,
        sum(f.airport_fee) as total_airport_fees,

        -- Per trip metrics
        avg(f.fare_amount) as avg_fare_per_trip,
        avg(f.tip_amount) as avg_tip_per_trip,
        avg(f.total_amount) as avg_total_per_trip,

        -- Peak vs. Off-peak metrics
        sum(case when t.is_rush_hour then f.total_amount else 0 end) as peak_revenue,
        sum(case when not t.is_rush_hour then f.total_amount else 0 end) as off_peak_revenue,
        count(case when t.is_rush_hour then 1 end) as peak_trips,
        count(case when not t.is_rush_hour then 1 end) as off_peak_trips,

        -- Time period metrics
        sum(case when t.period_of_day = 'Morning Rush' then f.total_amount else 0 end) as morning_rush_revenue,
        sum(case when t.period_of_day = 'Midday' then f.total_amount else 0 end) as midday_revenue,
        sum(case when t.period_of_day = 'Evening Rush' then f.total_amount else 0 end) as evening_rush_revenue,
        sum(case when t.period_of_day = 'Evening' then f.total_amount else 0 end) as evening_revenue,
        sum(case when t.period_of_day = 'Late Night/Early Morning' then f.total_amount else 0 end) as late_night_revenue,

        -- Payment analysis
        sum(case when pt.payment_desc = 'Credit card' then f.total_amount else 0 end) as credit_card_revenue,
        sum(case when pt.payment_desc = 'Cash' then f.total_amount else 0 end) as cash_revenue,

        -- Tip analysis
        sum(case when pt.payment_desc = 'Credit card' then f.tip_amount else 0 end) as credit_card_tips,
        sum(case when pt.payment_desc = 'Credit card' then 1 else 0 end) as credit_card_trips,
        case
            when sum(case when pt.payment_desc = 'Credit card' then 1 else 0 end) > 0
                then
                    sum(case when pt.payment_desc = 'Credit card' then f.tip_amount else 0 end)
                    / sum(case when pt.payment_desc = 'Credit card' then 1 else 0 end)
            else 0
        end as avg_credit_card_tip

    from {{ ref('fact_taxi_trip') }} f
    left join {{ ref('dim_date') }} d
        on f.date_key_pickup = d.date_key
    left join {{ ref('dim_time') }} t
        on f.time_key_pickup = t.time_key
    left join {{ ref('dim_vendor') }} v
        on f.vendor_key = v.vendor_key
    left join {{ ref('dim_location') }} l_pu
        on f.pu_location_key = l_pu.location_key
    left join {{ ref('dim_location') }} l_do
        on f.do_location_key = l_do.location_key
    left join {{ ref('dim_payment_type') }} pt
        on f.payment_type_key = pt.payment_type_key
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),

revenue_summary as (
    select
        trip_date,
        day_name,
        period_of_day,
        is_rush_hour,
        month_name,
        quarter_number,
        year_number,
        taxi_type,
        vendor_name,
        pickup_borough,
        dropoff_borough,
        payment_desc,

        -- Volume metrics
        total_trips,

        -- Revenue metrics
        total_fare_amount,
        total_tip_amount,
        total_revenue,

        -- Fee breakdown metrics
        total_extra_charges,
        total_mta_tax,
        total_tolls,
        total_improvement_surcharge,
        total_congestion_surcharge,
        total_airport_fees,

        -- Average metrics
        avg_fare_per_trip,
        avg_tip_per_trip,
        avg_total_per_trip,

        -- Fee composition analysis
        total_fare_amount / nullif(total_revenue, 0) as fare_percent_of_total,
        total_tip_amount / nullif(total_revenue, 0) as tip_percent_of_total,
        (
            total_extra_charges + total_mta_tax + total_improvement_surcharge
            + total_congestion_surcharge + total_airport_fees
        )
        / nullif(total_revenue, 0) as fees_percent_of_total,
        total_tolls / nullif(total_revenue, 0) as tolls_percent_of_total,

        -- Payment analysis
        credit_card_revenue,
        cash_revenue,
        credit_card_revenue / nullif(total_revenue, 0) as credit_card_percent,
        cash_revenue / nullif(total_revenue, 0) as cash_percent,

        -- Tip analysis
        credit_card_tips,
        credit_card_trips,
        avg_credit_card_tip,
        credit_card_tips / nullif(credit_card_revenue, 0) as tip_percent_on_card,

        -- Peak vs. Off-peak analysis
        peak_revenue,
        off_peak_revenue,
        peak_trips,
        off_peak_trips,
        peak_revenue / nullif(total_revenue, 0) as peak_revenue_percent,
        off_peak_revenue / nullif(total_revenue, 0) as off_peak_revenue_percent,
        peak_trips / nullif(total_trips, 0) as peak_trips_percent,
        off_peak_trips / nullif(total_trips, 0) as off_peak_trips_percent,

        -- Average metrics by peak/off-peak
        case when peak_trips > 0 then peak_revenue / peak_trips else 0 end as avg_fare_per_peak_trip,
        case when off_peak_trips > 0 then off_peak_revenue / off_peak_trips else 0 end as avg_fare_per_off_peak_trip,
        case
            when off_peak_trips > 0 and peak_trips > 0
                then (peak_revenue / peak_trips) / nullif((off_peak_revenue / off_peak_trips), 0)
            else 0
        end as peak_to_off_peak_fare_ratio,

        -- Time period revenue analysis
        morning_rush_revenue,
        midday_revenue,
        evening_rush_revenue,
        evening_revenue,
        late_night_revenue,

        -- Time period revenue percentages
        morning_rush_revenue / nullif(total_revenue, 0) as morning_rush_revenue_percent,
        midday_revenue / nullif(total_revenue, 0) as midday_revenue_percent,
        evening_rush_revenue / nullif(total_revenue, 0) as evening_rush_revenue_percent,
        evening_revenue / nullif(total_revenue, 0) as evening_revenue_percent,
        late_night_revenue / nullif(total_revenue, 0) as late_night_revenue_percent,

        -- Surge pricing effectiveness metrics
        (morning_rush_revenue + evening_rush_revenue) /
        nullif((midday_revenue + evening_revenue + late_night_revenue), 0) as rush_to_non_rush_revenue_ratio

    from revenue_by_day
)

select * from revenue_summary
