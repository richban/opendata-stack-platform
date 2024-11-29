-- This model selects all columns from the green_taxi_trip_bronze source

{{ config( materialized='view') }}

select *
from {{ source('taxi_trips_bronze', 'green_taxi_trip_bronze') }} 