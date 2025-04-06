{% macro incremental_backfill(date_column, taxi_type=none) %}
    {% if is_incremental() and not var('backfill_start_date', false) %}
        -- Normal incremental behavior - only process new data
        and date_trunc('month', {{ date_column }}) >= (
            select
                coalesce(
                    date_trunc('month', max(_incremental_timestamp)),
                    '2000-01-01'::date
                )
            from {{ this }}
            {% if taxi_type is not none %}
            where taxi_type = '{{ taxi_type }}'
            {% endif %}
        )
    {% elif is_incremental() and var('backfill_start_date', false) %}
        -- Backfill behavior - process specified date range
        and date_trunc('month', {{ date_column }}) >= cast('{{ var("backfill_start_date") }}' as date)
        and date_trunc('month', {{ date_column }}) <= cast('{{ var("backfill_end_date") }}' as date)
    {% endif %}
{% endmacro %}
