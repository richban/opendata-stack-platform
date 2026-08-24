CREATE TABLE dim_date (
    date_key int PRIMARY KEY,
    full_date date NOT NULL,
    day_of_week varchar,
    day_of_month int,
    day_name varchar,
    week_of_year int,
    month_number int,
    month_name varchar,
    quarter_number int,
    year_number int,
    row_effective_date date,
    row_expiration_date date,
    current_flag char(1) DEFAULT 'Y'
);

CREATE TABLE dim_location (
    location_key bigint PRIMARY KEY,
    location_id int NOT NULL,
    borough varchar(50),
    zone_name varchar(100),
    row_effective_date date,
    row_expiration_date date,
    current_flag char(1) DEFAULT 'Y'
);

CREATE TABLE dim_vendor (
    vendor_key bigint PRIMARY KEY,
    vendor_id varchar(20) NOT NULL,
    vendor_name varchar(100),
    row_effective_date date,
    row_expiration_date date,
    current_flag char(1) DEFAULT 'Y'
);

CREATE TABLE dim_rate_code (
    rate_code_key bigint PRIMARY KEY,
    rate_code_id int NOT NULL,
    rate_code_desc varchar(100),
    row_effective_date date,
    row_expiration_date date,
    current_flag char(1) DEFAULT 'Y'
);

CREATE TABLE dim_payment_type (
    payment_type_key bigint PRIMARY KEY,
    payment_type_id int NOT NULL,
    payment_desc varchar(50),
    row_effective_date date,
    row_expiration_date date,
    current_flag char(1) DEFAULT 'Y'
);

CREATE TABLE dim_trip_type (
    trip_type_key bigint PRIMARY KEY,
    trip_type_id int NOT NULL,
    trip_type_desc varchar(50),
    row_effective_date date,
    row_expiration_date date,
    current_flag char(1) DEFAULT 'Y'
);

CREATE TABLE fact_taxi_trip (
    taxi_trip_key bigint PRIMARY KEY,
    date_key_pickup int NOT NULL,
    date_key_dropoff int NOT NULL,
    pu_location_key bigint NOT NULL,
    do_location_key bigint NOT NULL,
    vendor_key bigint NOT NULL,
    rate_code_key bigint NOT NULL,
    payment_type_key bigint NOT NULL,
    trip_type_key bigint,
    passenger_count int,
    trip_distance numeric(9, 2),
    fare_amount numeric(9, 2),
    extra numeric(9, 2),
    mta_tax numeric(9, 2),
    improvement_surcharge numeric(9, 2),
    tip_amount numeric(9, 2),
    tolls_amount numeric(9, 2),
    congestion_surcharge numeric(9, 2),
    airport_fee numeric(9, 2),
    total_amount numeric(9, 2),
    store_and_fwd_flag char(1),
    hvfhs_license_num varchar(20),
    dispatching_base_num varchar(20),
    originating_base_num varchar(20),
    request_datetime timestamp,
    on_scene_datetime timestamp,
    trip_miles numeric(9, 2),
    trip_time int,
    base_passenger_fare numeric(9, 2),
    bcf numeric(9, 2),
    sales_tax numeric(9, 2),
    driver_pay numeric(9, 2),
    shared_request_flag char(1),
    shared_match_flag char(1),
    access_a_ride_flag char(1),
    wav_request_flag char(1),
    wav_match_flag char(1),
    record_loaded_timestamp timestamp DEFAULT 'CURRENT_TIMESTAMP'
);

-- Comments for dim tables
COMMENT ON COLUMN dim_date.date_key IS 'e.g., 20230201 for 2023-02-01';

COMMENT ON COLUMN dim_location.location_id IS
    'TLC Zone ID from PULocationID or DOLocationID';

COMMENT ON COLUMN dim_vendor.vendor_id IS 'e.g., 1, 2, HV0003, etc.';

COMMENT ON COLUMN dim_rate_code.rate_code_id IS
    '1=Standard,2=JFK,3=Newark,...';

COMMENT ON COLUMN dim_payment_type.payment_type_id IS
    '1=Credit,2=Cash,3=No charge, etc.';

COMMENT ON COLUMN dim_trip_type.trip_type_id IS
    '1=Street-hail,2=Dispatch (Green)';

-- Comments for fact table keys
COMMENT ON COLUMN fact_taxi_trip.taxi_trip_key IS
    'Surrogate key for the fact table';

COMMENT ON COLUMN fact_taxi_trip.date_key_pickup IS 'FK to dim_date.date_key';

COMMENT ON COLUMN fact_taxi_trip.date_key_dropoff IS 'FK to dim_date.date_key';

COMMENT ON COLUMN fact_taxi_trip.pu_location_key IS
    'FK to dim_location.location_key';

COMMENT ON COLUMN fact_taxi_trip.do_location_key IS
    'FK to dim_location.location_key';

COMMENT ON COLUMN fact_taxi_trip.vendor_key IS 'FK to dim_vendor.vendor_key';

COMMENT ON COLUMN fact_taxi_trip.rate_code_key IS
    'FK to dim_rate_code.rate_code_key';

COMMENT ON COLUMN fact_taxi_trip.payment_type_key IS
    'FK to dim_payment_type.payment_type_key';

COMMENT ON COLUMN fact_taxi_trip.trip_type_key IS
    'FK to dim_trip_type.trip_type_key (Green/HV only)';

-- Comments for trip metrics
COMMENT ON COLUMN fact_taxi_trip.trip_distance IS
    'Yellow/Green label for distance';

COMMENT ON COLUMN fact_taxi_trip.fare_amount IS
    'Taximeter fare or unified base fare';

COMMENT ON COLUMN fact_taxi_trip.extra IS
    '$0.50, $1 rush hour/overnight, etc. (Yellow/Green)';

COMMENT ON COLUMN fact_taxi_trip.tip_amount IS 'tips or credit card tips';

COMMENT ON COLUMN fact_taxi_trip.tolls_amount IS 'total tolls paid in the trip';

COMMENT ON COLUMN fact_taxi_trip.total_amount IS
    'total charged to passenger, not including cash tip';

COMMENT ON COLUMN fact_taxi_trip.store_and_fwd_flag IS
    'Y/N if trip was stored before forward';

-- Comments for HVFHS specific fields
COMMENT ON COLUMN fact_taxi_trip.hvfhs_license_num IS
    'HVFHS base license # (Uber, Lyft, etc.)';

COMMENT ON COLUMN fact_taxi_trip.dispatching_base_num IS
    'Base that dispatched the trip';

COMMENT ON COLUMN fact_taxi_trip.originating_base_num IS
    'Base that received the original request';

COMMENT ON COLUMN fact_taxi_trip.request_datetime IS
    'When passenger requested the ride';

COMMENT ON COLUMN fact_taxi_trip.on_scene_datetime IS
    'When driver arrived (Accessible Vehicles only)';

COMMENT ON COLUMN fact_taxi_trip.trip_miles IS
    'HVFHV label for distance, unify if desired';

COMMENT ON COLUMN fact_taxi_trip.trip_time IS
    'total seconds for passenger trip (HVFHV)';

COMMENT ON COLUMN fact_taxi_trip.base_passenger_fare IS
    'HVFHV fare before tolls/tips/taxes';

COMMENT ON COLUMN fact_taxi_trip.bcf IS 'Black Car Fund contribution';

COMMENT ON COLUMN fact_taxi_trip.sales_tax IS 'NYS sales tax portion';

COMMENT ON COLUMN fact_taxi_trip.driver_pay IS
    'net driver pay, excluding tolls/tips';

-- Comments for flags
COMMENT ON COLUMN fact_taxi_trip.shared_request_flag IS
    'Y/N: passenger consented to a shared/pooled ride';

COMMENT ON COLUMN fact_taxi_trip.shared_match_flag IS
    'Y/N: passenger actually shared the ride w/ another passenger';

COMMENT ON COLUMN fact_taxi_trip.access_a_ride_flag IS
    'Y/N: MTA Access-a-Ride trip';

COMMENT ON COLUMN fact_taxi_trip.wav_request_flag IS
    'Y/N: wheelchair-accessible vehicle requested';

COMMENT ON COLUMN fact_taxi_trip.wav_match_flag IS 'Y/N: trip occurred in a WAV';

-- Foreign key constraints
ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    date_key_pickup
) REFERENCES dim_date (date_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    date_key_dropoff
) REFERENCES dim_date (date_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    pu_location_key
) REFERENCES dim_location (location_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    do_location_key
) REFERENCES dim_location (location_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    vendor_key
) REFERENCES dim_vendor (vendor_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    rate_code_key
) REFERENCES dim_rate_code (rate_code_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    payment_type_key
) REFERENCES dim_payment_type (payment_type_key);

ALTER TABLE fact_taxi_trip ADD FOREIGN KEY (
    trip_type_key
) REFERENCES dim_trip_type (trip_type_key);
