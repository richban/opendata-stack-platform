{{ config(
    materialized='incremental',
    partition_by={
        "field": "date_partition",
        "data_type": "date",
        "granularity": "month",
    }
) }}

select *
from {{ source('taxi_trips_bronze', 'green_taxi_trip_bronze') }}
{% if is_incremental() %}
where date_partition = CAST('{{ var("date_partition") }}' AS DATE)
{% endif %}