# Table Privileges Automation

# Grant all privileges on existing tables in MAIN schema to ACCOUNTADMIN
resource "snowflake_grant_privileges_to_account_role" "main_existing_tables_admin" {
  account_role_name = "ACCOUNTADMIN"
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.nyc_database.name}\".\"${snowflake_schema.main.name}\""
    }
  }
}

# Grant all privileges on future tables in MAIN schema to ACCOUNTADMIN
resource "snowflake_grant_privileges_to_account_role" "main_future_tables_admin" {
  account_role_name = "ACCOUNTADMIN"
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.nyc_database.name}\".\"${snowflake_schema.main.name}\""
    }
  }
}

# Grant all privileges on MAIN schema tables to Data Engineer Role (for data loading and transformations)
resource "snowflake_grant_privileges_to_account_role" "main_all_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.nyc_database.name}\".\"${snowflake_schema.main.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "main_all_future_data_engineer" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.nyc_database.name}\".\"${snowflake_schema.main.name}\""
    }
  }
}

# Create a null resource to handle dynamic schema privileges
resource "null_resource" "dynamic_schema_privileges" {
  # This ensures privileges are granted after all schemas are created
  triggers = {
    database_name = snowflake_database.nyc_database.name
    timestamp     = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Setting up dynamic privileges for database: ${snowflake_database.nyc_database.name}"
      echo "Data Engineer Role: ${snowflake_account_role.data_engineer_role.name}"
      echo ""
      echo "Manual steps (if needed):"
      echo "1. Grant privileges on bronze schemas to DATA_ENGINEER_ROLE"
      echo "2. Monitor and adjust privileges as new schemas are created"
    EOT
  }

  depends_on = [
    snowflake_database.nyc_database,
    snowflake_schema.main,
    snowflake_account_role.data_engineer_role,
  ]
}

# Note: Privilege monitoring view removed due to Snowflake information_schema limitations
# To monitor privileges, use:
# SHOW GRANTS ON DATABASE NYC_DATABASE;
# SHOW GRANTS ON SCHEMA NYC_DATABASE.MAIN;
# SHOW GRANTS TO ROLE DATA_ENGINEER_ROLE;
