# Variables for Snowflake S3 Storage Integration

variable "aws_region" {
  description = "AWS region where resources will be created"
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Name of the project (used for resource naming)"
  type        = string
  default     = "opendata-stack"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# Snowflake Connection Variables
variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_admin_user" {
  description = "Snowflake admin user for Terraform operations"
  type        = string
}

variable "snowflake_admin_password" {
  description = "Password for Snowflake admin user"
  type        = string
  sensitive   = true
}

# Snowflake AWS Integration Variables (from DESC INTEGRATION)
# These are optional for first run, required for second run
variable "snowflake_aws_account_id" {
  description = "AWS account ID from STORAGE_AWS_IAM_USER_ARN (DESC INTEGRATION output)"
  type        = string
  default     = null
}

variable "snowflake_external_id" {
  description = "External ID from STORAGE_AWS_EXTERNAL_ID (DESC INTEGRATION output)"
  type        = string
  default     = null
}

variable "snowflake_aws_user" {
  description = "AWS user from STORAGE_AWS_IAM_USER_ARN (DESC INTEGRATION output)"
  type        = string
  default     = null
}

# Snowflake Database Configuration
variable "snowflake_database_name" {
  description = "Name of the Snowflake database"
  type        = string
  default     = "NYC_DATABASE"
}

variable "snowflake_warehouse_name" {
  description = "Name of the Snowflake warehouse"
  type        = string
  default     = "COMPUTE_WH"
}

# Data Engineer User Configuration
variable "data_engineer_username" {
  description = "Username for data engineer user"
  type        = string
  default     = "DATA_ENGINEER"
}

variable "data_engineer_password" {
  description = "Password for data engineer user"
  type        = string
  sensitive   = true
}

variable "data_engineer_email" {
  description = "Email for data engineer user"
  type        = string
  default     = ""
}


# Warehouse Configuration
variable "warehouse_size" {
  description = "Size of the Snowflake warehouse"
  type        = string
  default     = "X-SMALL"
}

variable "warehouse_auto_suspend" {
  description = "Number of seconds to auto-suspend warehouse when idle"
  type        = number
  default     = 60
}
