-- Generic audit for checking unique constraints
AUDIT (
  name assert_unique_key,
  defaults (
    key_column = id
  )
);
SELECT @key_column, COUNT(*) as duplicate_count
FROM @this_model
GROUP BY @key_column
HAVING COUNT(*) > 1;

-- Generic audit for checking not null constraints
AUDIT (
  name assert_not_null,
  defaults (
    column_name = id
  )
);
SELECT *
FROM @this_model
WHERE @column_name IS NULL;

-- Generic audit for valid range checks
AUDIT (
  name assert_valid_range,
  defaults (
    column_name = value,
    min_value = 0,
    max_value = 100
  )
);
SELECT *
FROM @this_model
WHERE @column_name < @min_value OR @column_name > @max_value;

-- Audit for checking foreign key relationships
AUDIT (
  name assert_foreign_key_exists,
  defaults (
    fk_column = foreign_key,
    ref_table = 'reference_table',
    ref_column = 'id'
  )
);
SELECT f.*
FROM @this_model f
LEFT JOIN @ref_table r ON f.@fk_column = r.@ref_column
WHERE f.@fk_column IS NOT NULL AND r.@ref_column IS NULL;

-- Specific audit for trip duration validation
AUDIT (
  name assert_valid_trip_duration
);
SELECT taxi_trip_key, pickup_datetime, dropoff_datetime
FROM @this_model
WHERE dropoff_datetime < pickup_datetime;

-- Specific audit for FHVHV total amount consistency
AUDIT (
  name assert_fhvhv_total_amount_consistency
);
SELECT 
  taxi_trip_key,
  total_amount,
  (COALESCE(fare_amount, 0) + COALESCE(tip_amount, 0) + COALESCE(tolls_amount, 0) + COALESCE(congestion_surcharge, 0)) as calculated_total,
  ABS(total_amount - (COALESCE(fare_amount, 0) + COALESCE(tip_amount, 0) + COALESCE(tolls_amount, 0) + COALESCE(congestion_surcharge, 0))) as difference
FROM @this_model
WHERE taxi_type = 'fhvhv'
  AND ABS(total_amount - (
    COALESCE(fare_amount, 0) + 
    COALESCE(tip_amount, 0) + 
    COALESCE(tolls_amount, 0) + 
    COALESCE(congestion_surcharge, 0)
  )) >= 0.01;

-- Audit for non-negative amounts
AUDIT (
  name assert_non_negative_amount,
  defaults (
    amount_column = total_amount
  )
);
SELECT *
FROM @this_model
WHERE @amount_column < 0;