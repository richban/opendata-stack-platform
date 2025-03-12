{{ config(
    materialized='incremental',
    unique_key=['vendor_id', 'lpep_pickup_datetime', 'pu_location_id', 'do_location_id'],
    incremental_strategy='delete+insert',
    partition_by={
        "field": "partition_key",
        "data_type": "date",
        "granularity": "month",
    }
) }}

select *
from {{ source('green_taxi_trip_silver', 'green_taxi_trip_silver') }}
{% if is_incremental() %}
    where partition_key = cast('{{ var("partition_key") }}' as date)
{% endif %}
