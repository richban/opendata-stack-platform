# Outputs for Snowflake Storage Integration Configuration

output "s3_bucket_name" {
  description = "Name of the S3 bucket created for Snowflake"
  value       = aws_s3_bucket.snowflake_data.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.snowflake_data.arn
}

output "s3_bucket_url" {
  description = "S3 bucket URL for Snowflake storage integration"
  value       = "s3://${aws_s3_bucket.snowflake_data.id}"
}

output "iam_role_arn" {
  description = "ARN of the IAM role for Snowflake"
  value       = aws_iam_role.snowflake_role.arn
}

output "iam_role_name" {
  description = "Name of the IAM role for Snowflake"
  value       = aws_iam_role.snowflake_role.name
}

output "aws_account_id" {
  description = "AWS Account ID"
  value       = data.aws_caller_identity.current.account_id
}


# Snowflake Storage Integration SQL Commands
# Snowflake Resource Outputs
output "snowflake_database_name" {
  description = "Name of the created Snowflake database"
  value       = snowflake_database.nyc_database.name
}

output "snowflake_warehouse_name" {
  description = "Name of the created Snowflake warehouse"
  value       = snowflake_warehouse.compute_wh.name
}

output "snowflake_storage_integration_name" {
  description = "Name of the created storage integration"
  value       = snowflake_storage_integration.s3_integration.name
}

output "STORAGE_AWS_IAM_USER_ARN" {
  description = "AWS IAM user ARN from Snowflake storage integration (use this for IAM trust policy)"
  value       = snowflake_storage_integration.s3_integration.storage_aws_iam_user_arn
}

output "STORAGE_AWS_EXTERNAL_ID" {
  description = "AWS External ID from Snowflake storage integration (use this for IAM trust policy)"
  value       = snowflake_storage_integration.s3_integration.storage_aws_external_id
}

output "snowflake_data_engineer_role" {
  description = "Name of the Data Engineer role"
  value       = snowflake_account_role.data_engineer_role.name
}

output "snowflake_data_engineer_user" {
  description = "Name of the Data Engineer user"
  value       = snowflake_user.data_engineer.name
}

output "snowflake_stages" {
  description = "Names of created Snowflake stages"
  value = {
    raw_stage     = snowflake_stage.raw_stage.name
    dlt_s3_stage  = snowflake_stage.dlt_s3_stage.name
  }
}

# Connection Information for Applications
output "connection_info" {
  description = "Connection information for applications"
  value = {
    # For .env.prod file
    snowflake_account   = var.snowflake_account
    snowflake_database  = snowflake_database.nyc_database.name
    snowflake_warehouse = snowflake_warehouse.compute_wh.name
    data_engineer_user  = snowflake_user.data_engineer.name
    data_engineer_role  = snowflake_account_role.data_engineer_role.name

    # S3 information
    s3_bucket           = aws_s3_bucket.snowflake_data.id
    s3_bucket_prefix    = "s3://${aws_s3_bucket.snowflake_data.id}"
    aws_region          = var.aws_region
  }
  sensitive = false
}

# Manual verification commands
output "verification_commands" {
  description = "Commands to verify the setup"
  value = <<-EOT
    # Verify Snowflake setup
    USE ROLE ACCOUNTADMIN;
    DESC INTEGRATION ${snowflake_storage_integration.s3_integration.name};
    SHOW STAGES IN SCHEMA ${snowflake_database.nyc_database.name}.PUBLIC;
    SHOW ROLES;
    SHOW USERS;

    # Test S3 access
    aws s3 ls s3://${aws_s3_bucket.snowflake_data.id}/

    # Test connection with Data Engineer user
    USE ROLE ${snowflake_account_role.data_engineer_role.name};
    USE DATABASE ${snowflake_database.nyc_database.name};
    SHOW STAGES;

    # Verify Trust Policy
    STORAGE_AWS_IAM_USER_ARN="${snowflake_storage_integration.s3_integration.storage_aws_iam_user_arn}"
    STORAGE_AWS_EXTERNAL_ID="${snowflake_storage_integration.s3_integration.storage_aws_external_id}"
  EOT
}
