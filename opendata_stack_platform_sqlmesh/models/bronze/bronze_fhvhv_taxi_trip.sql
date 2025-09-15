MODEL (
  name taxi.bronze_fhvhv_taxi_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  grain (row_hash),
);

SELECT
  c.*,
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  bronze_fhvhv.fhvhv_taxi_trip as c
WHERE
  TO_TIMESTAMP(CAST(c._dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
