# Snowflake Storage Integration Configuration

# Create Storage Integration (initial creation with placeholder IAM role)
resource "snowflake_storage_integration" "s3_integration" {
  name    = "${var.project_name}_s3_integration"
  comment = "S3 storage integration for data platform"
  type    = "EXTERNAL_STAGE"

  enabled = true

  storage_provider         = "S3"
  storage_aws_role_arn     = aws_iam_role.snowflake_role.arn
  storage_allowed_locations = [
    "s3://${aws_s3_bucket.snowflake_data.id}/",
    "s3://${aws_s3_bucket.snowflake_data.id}/*"
  ]

  depends_on = [
    aws_iam_role.snowflake_role,
    aws_s3_bucket.snowflake_data
  ]
}

# Grant usage on integration to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "data_engineer_integration" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_storage_integration.s3_integration.name
  }
}

# Data source to get integration details after creation
data "snowflake_system_get_aws_sns_iam_policy" "s3_integration_policy" {
  aws_sns_topic_arn = "arn:aws:sns:${var.aws_region}:${data.aws_caller_identity.current.account_id}:snowflake-integration"
}

# NOTE: After the initial apply, we need to update the IAM trust policy
# with the values from DESC INTEGRATION. This is handled by the null_resource below.

# Get integration description to extract AWS user ARN and External ID
resource "null_resource" "get_integration_details" {
  triggers = {
    integration_name = snowflake_storage_integration.s3_integration.name
  }

  provisioner "local-exec" {
    command = <<-EOT
      # This script extracts the integration details
      # Note: In production, you might want to use a more robust method
      echo "Integration created: ${snowflake_storage_integration.s3_integration.name}"
      echo "Manual step required: Run 'DESC INTEGRATION ${snowflake_storage_integration.s3_integration.name};' in Snowflake"
      echo "Then update the terraform.tfvars file with the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values"
    EOT
  }

  depends_on = [
    snowflake_storage_integration.s3_integration
  ]
}
