# Snowflake Database and Warehouse Configuration

# Create Warehouse
resource "snowflake_warehouse" "compute_wh" {
  name           = var.snowflake_warehouse_name
  warehouse_size = var.warehouse_size
  auto_suspend   = var.warehouse_auto_suspend
  auto_resume    = true
  comment        = "Warehouse for data platform operations"

  # Optional: scaling settings
  min_cluster_count         = 1
  max_cluster_count         = 1
  scaling_policy           = "STANDARD"
  initially_suspended      = false
  enable_query_acceleration = false
}

# Create Database
resource "snowflake_database" "nyc_database" {
  name    = var.snowflake_database_name
  comment = "NYC taxi data platform database"

  # Optional: data retention settings
  data_retention_time_in_days = 7
}

# Create MAIN schema
resource "snowflake_schema" "main" {
  database = snowflake_database.nyc_database.name
  name     = "MAIN"
  comment  = "Main schema for processed data"
}

# Grant USAGE on PUBLIC schema to Data Engineer Role (PUBLIC schema exists by default)
resource "snowflake_grant_privileges_to_account_role" "public_usage_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.nyc_database.name}\".\"PUBLIC\""
  }
}

# Grant CREATE STAGE on PUBLIC schema to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "public_create_stage_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.nyc_database.name}\".\"PUBLIC\""
  }
}

# Grant database usage to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "data_engineer_database_usage" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_database.name
  }
}

# Grant database create schema to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "data_engineer_create_schema" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.nyc_database.name
  }
}

# Grant privileges on existing MAIN schema to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "main_schema_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.nyc_database.name}\".\"${snowflake_schema.main.name}\""
  }
}

# Grant future schema privileges to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "data_engineer_future_schemas" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
  on_schema {
    future_schemas_in_database = snowflake_database.nyc_database.name
  }
}

# Grant future table privileges to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "data_engineer_future_tables" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.nyc_database.name
    }
  }
}
