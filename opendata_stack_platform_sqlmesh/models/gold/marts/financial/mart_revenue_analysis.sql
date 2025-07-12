MODEL (
  name taxi.mart_revenue_analysis,
  kind FULL
);

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

WITH revenue_by_day AS (
    SELECT
        d.full_date AS trip_date,
        d.day_name,
        t.period_of_day,
        t.is_peak_time,
        d.month_name,
        d.quarter_number,
        d.year_number,
        f.taxi_type,
        v.vendor_name,
        l_pu.borough_name AS pickup_borough,
        l_do.borough_name AS dropoff_borough,
        pt.payment_desc,

        -- Revenue metrics
        COUNT(*) AS total_trips,
        SUM(f.fare_amount) AS total_fare_amount,
        SUM(f.tip_amount) AS total_tip_amount,
        SUM(f.total_amount) AS total_revenue,
        SUM(f.extra) AS total_extra_charges,
        SUM(f.mta_tax) AS total_mta_tax,
        SUM(f.tolls_amount) AS total_tolls,
        SUM(f.improvement_surcharge) AS total_improvement_surcharge,
        SUM(f.congestion_surcharge) AS total_congestion_surcharge,
        SUM(f.airport_fee) AS total_airport_fees,

        -- Per trip metrics
        AVG(f.fare_amount) AS avg_fare_per_trip,
        AVG(f.tip_amount) AS avg_tip_per_trip,
        AVG(f.total_amount) AS avg_total_per_trip,

        -- Peak vs. Off-peak metrics
        SUM(CASE WHEN t.is_peak_time THEN f.total_amount ELSE 0 END) AS peak_revenue,
        SUM(CASE WHEN NOT t.is_peak_time THEN f.total_amount ELSE 0 END) AS off_peak_revenue,
        COUNT(CASE WHEN t.is_peak_time THEN 1 END) AS peak_trips,
        COUNT(CASE WHEN NOT t.is_peak_time THEN 1 END) AS off_peak_trips,

        -- Time period metrics
        SUM(CASE WHEN t.period_of_day = 'Morning Rush' THEN f.total_amount ELSE 0 END) AS morning_rush_revenue,
        SUM(CASE WHEN t.period_of_day = 'Midday' THEN f.total_amount ELSE 0 END) AS midday_revenue,
        SUM(CASE WHEN t.period_of_day = 'Evening Rush' THEN f.total_amount ELSE 0 END) AS evening_rush_revenue,
        SUM(CASE WHEN t.period_of_day = 'Evening' THEN f.total_amount ELSE 0 END) AS evening_revenue,
        SUM(CASE WHEN t.period_of_day = 'Late Night/Early Morning' THEN f.total_amount ELSE 0 END) AS late_night_revenue,

        -- Payment analysis
        SUM(CASE WHEN pt.payment_desc = 'Credit card' THEN f.total_amount ELSE 0 END) AS credit_card_revenue,
        SUM(CASE WHEN pt.payment_desc = 'Cash' THEN f.total_amount ELSE 0 END) AS cash_revenue,

        -- Tip analysis
        SUM(CASE WHEN pt.payment_desc = 'Credit card' THEN f.tip_amount ELSE 0 END) AS credit_card_tips,
        SUM(CASE WHEN pt.payment_desc = 'Credit card' THEN 1 ELSE 0 END) AS credit_card_trips,
        CASE
            WHEN SUM(CASE WHEN pt.payment_desc = 'Credit card' THEN 1 ELSE 0 END) > 0
                THEN
                    SUM(CASE WHEN pt.payment_desc = 'Credit card' THEN f.tip_amount ELSE 0 END)
                    / SUM(CASE WHEN pt.payment_desc = 'Credit card' THEN 1 ELSE 0 END)
            ELSE 0
        END AS avg_credit_card_tip

    FROM taxi.fact_taxi_trip f
    LEFT JOIN taxi.dim_date d
        ON f.date_key_pickup = d.date_key
    LEFT JOIN taxi.dim_time t
        ON f.time_key_pickup = t.time_key
    LEFT JOIN taxi.dim_vendor v
        ON f.vendor_key = v.vendor_key
    LEFT JOIN taxi.dim_location l_pu
        ON f.pu_location_key = l_pu.location_key
    LEFT JOIN taxi.dim_location l_do
        ON f.do_location_key = l_do.location_key
    LEFT JOIN taxi.dim_payment_type pt
        ON f.payment_type_key = pt.payment_type_key
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),

revenue_summary AS (
    SELECT
        trip_date,
        day_name,
        period_of_day,
        is_peak_time,
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
        total_fare_amount / NULLIF(total_revenue, 0) AS fare_percent_of_total,
        total_tip_amount / NULLIF(total_revenue, 0) AS tip_percent_of_total,
        (
            total_extra_charges + total_mta_tax + total_improvement_surcharge
            + total_congestion_surcharge + total_airport_fees
        )
        / NULLIF(total_revenue, 0) AS fees_percent_of_total,
        total_tolls / NULLIF(total_revenue, 0) AS tolls_percent_of_total,

        -- Payment analysis
        credit_card_revenue,
        cash_revenue,
        credit_card_revenue / NULLIF(total_revenue, 0) AS credit_card_percent,
        cash_revenue / NULLIF(total_revenue, 0) AS cash_percent,

        -- Tip analysis
        credit_card_tips,
        credit_card_trips,
        avg_credit_card_tip,
        credit_card_tips / NULLIF(credit_card_revenue, 0) AS tip_percent_on_card,

        -- Peak vs. Off-peak analysis
        peak_revenue,
        off_peak_revenue,
        peak_trips,
        off_peak_trips,
        peak_revenue / NULLIF(total_revenue, 0) AS peak_revenue_percent,
        off_peak_revenue / NULLIF(total_revenue, 0) AS off_peak_revenue_percent,
        peak_trips / NULLIF(total_trips, 0) AS peak_trips_percent,
        off_peak_trips / NULLIF(total_trips, 0) AS off_peak_trips_percent,

        -- Average metrics by peak/off-peak
        CASE WHEN peak_trips > 0 THEN peak_revenue / peak_trips ELSE 0 END AS avg_fare_per_peak_trip,
        CASE WHEN off_peak_trips > 0 THEN off_peak_revenue / off_peak_trips ELSE 0 END AS avg_fare_per_off_peak_trip,
        CASE
            WHEN off_peak_trips > 0 AND peak_trips > 0
                THEN (peak_revenue / peak_trips) / NULLIF((off_peak_revenue / off_peak_trips), 0)
            ELSE 0
        END AS peak_to_off_peak_fare_ratio,

        -- Time period revenue analysis
        morning_rush_revenue,
        midday_revenue,
        evening_rush_revenue,
        evening_revenue,
        late_night_revenue,

        -- Time period revenue percentages
        morning_rush_revenue / NULLIF(total_revenue, 0) AS morning_rush_revenue_percent,
        midday_revenue / NULLIF(total_revenue, 0) AS midday_revenue_percent,
        evening_rush_revenue / NULLIF(total_revenue, 0) AS evening_rush_revenue_percent,
        evening_revenue / NULLIF(total_revenue, 0) AS evening_revenue_percent,
        late_night_revenue / NULLIF(total_revenue, 0) AS late_night_revenue_percent,

        -- Surge pricing effectiveness metrics
        (morning_rush_revenue + evening_rush_revenue)
        / NULLIF((midday_revenue + evening_revenue + late_night_revenue), 0) AS rush_to_non_rush_revenue_ratio

    FROM revenue_by_day
)

SELECT * FROM revenue_summary