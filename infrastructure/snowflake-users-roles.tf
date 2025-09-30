# Snowflake Users and Roles Configuration

# Create Data Engineer User
resource "snowflake_user" "data_engineer" {
  name         = var.data_engineer_username
  password     = var.data_engineer_password
  email        = var.data_engineer_email
  display_name = "Data Engineer User"
  comment      = "User for data loading and transformation operations"

  default_warehouse = snowflake_warehouse.compute_wh.name
  default_role      = snowflake_account_role.data_engineer_role.name
}


# Create Data Engineer Role
resource "snowflake_account_role" "data_engineer_role" {
  name    = "DATA_ENGINEER_ROLE"
  comment = "Role for data loading and transformation operations"
}

# Grant Data Engineer Role to User
resource "snowflake_grant_account_role" "data_engineer_user_grant" {
  role_name = snowflake_account_role.data_engineer_role.name
  user_name = snowflake_user.data_engineer.name
}

# Note: Grant Data Engineer Role to Admin User manually if needed
# The admin user (var.snowflake_admin_user) may not exist as a Snowflake user
# To grant this role to your admin user, run:
# GRANT ROLE DATA_ENGINEER_ROLE TO USER <your_admin_username>;

# Grant warehouse usage to Data Engineer Role
resource "snowflake_grant_privileges_to_account_role" "data_engineer_warehouse" {
  account_role_name = snowflake_account_role.data_engineer_role.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.compute_wh.name
  }
}
