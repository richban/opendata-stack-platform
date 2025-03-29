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
from {{ source('silver_green', 'green_taxi_trip') }}
{% if is_incremental() %}
    where partition_key = cast('{{ var("backfill_start_date") }}' as date)
{% endif %}
