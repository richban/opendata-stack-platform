INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
INSTALL ducklake;
LOAD ducklake;

SET s3_region = 'us-east-1';
SET s3_access_key_id = 'miniouser';
SET s3_secret_access_key = 'miniouser';
SET s3_endpoint = '127.0.0.1:9000';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

ATTACH 'ducklake:./data/analytics_metadata.ducklake' AS ducklake (DATA_PATH 's3://warehouse');

SELECT * FROM ducklake.analytics__dev.mart_zone_analysis LIMIT 10;
