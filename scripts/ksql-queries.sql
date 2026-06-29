-- Force the server to process all historical messages from the beginning
SET 'auto.offset.reset'='earliest';

-- 1. Register the source stream (including raw 'ts' column)
CREATE STREAM IF NOT EXISTS listen_events_stream (
    ts BIGINT,  -- Raw client event timestamp in milliseconds
    userId BIGINT,
    firstName VARCHAR,
    lastName VARCHAR,
    gender VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR
) WITH (
    KAFKA_TOPIC='listen_events',
    VALUE_FORMAT='JSON'
);

-- 2. Replicate and serialize to AVRO, adding readable event and ingestion times
CREATE STREAM IF NOT EXISTS user_profiles WITH (KAFKA_TOPIC='user_profiles', VALUE_FORMAT='AVRO') AS
SELECT 
    userId, 
    firstName, 
    lastName, 
    gender, 
    city, 
    state, 
    zip,
    -- 1. Event Time: Converts the client's payload 'ts' to a readable timestamp
    FROM_UNIXTIME(ts) AS event_time,
    -- 2. Ingestion Time: Converts Kafka's record metadata ROWTIME to a readable timestamp
    FROM_UNIXTIME(ROWTIME) AS ingestion_time
FROM listen_events_stream
EMIT CHANGES;