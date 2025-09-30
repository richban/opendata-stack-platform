# Terraform configuration for Snowflake S3 Storage Integration
# This creates the AWS resources needed for Snowflake to access S3

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.94"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Purpose     = "snowflake-integration"
    }
  }
}

# Configure the Snowflake Provider
provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_admin_user
  password = var.snowflake_admin_password
  role     = "ACCOUNTADMIN"
}

# Random suffix for unique naming
resource "random_id" "suffix" {
  byte_length = 4
}

# Data source to get current AWS account ID
data "aws_caller_identity" "current" {}


locals {
  bucket_name = "${var.project_name}-${var.environment}-${random_id.suffix.hex}"
}
