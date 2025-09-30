# Snowflake Stages Configuration

# Create RAW_STAGE for CSV files
resource "snowflake_stage" "raw_stage" {
  name     = "RAW_STAGE"
  database = snowflake_database.nyc_database.name
  schema   = "PUBLIC"

  url = "s3://${aws_s3_bucket.snowflake_data.id}/raw/"

  storage_integration = snowflake_storage_integration.s3_integration.name
  file_format         = "TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"

  comment = "Stage for raw CSV data files"

  depends_on = [
    snowflake_storage_integration.s3_integration
  ]
}

# Create DLT_S3_STAGE for Parquet files
resource "snowflake_stage" "dlt_s3_stage" {
  name     = "DLT_S3_STAGE"
  database = snowflake_database.nyc_database.name
  schema   = "PUBLIC"

  url = "s3://${aws_s3_bucket.snowflake_data.id}/dlt_stage/"

  storage_integration = snowflake_storage_integration.s3_integration.name
  file_format         = "TYPE = PARQUET"

  comment = "Stage for DLT processed parquet files"

  depends_on = [
    snowflake_storage_integration.s3_integration
  ]
}

# Grant stage privileges to Data Engineer Role - RAW_STAGE
resource "snowflake_grant_privileges_to_account_role" "raw_stage_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE", "READ", "WRITE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.nyc_database.name}\".\"PUBLIC\".\"${snowflake_stage.raw_stage.name}\""
  }
}

# Grant stage privileges to Data Engineer Role - DLT_S3_STAGE
resource "snowflake_grant_privileges_to_account_role" "dlt_stage_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE", "READ", "WRITE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.nyc_database.name}\".\"PUBLIC\".\"${snowflake_stage.dlt_s3_stage.name}\""
  }
}