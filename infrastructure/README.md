# Snowflake S3 Storage Integration Infrastructure

This Terraform configuration automates the setup of AWS resources needed for Snowflake S3 storage integration.

## What This Creates

### AWS Resources
- **S3 Bucket**: Secure bucket with versioning and encryption for data storage
- **IAM Role**: Role that Snowflake can assume to access S3
- **IAM Policy**: Permissions for S3 access (ListBucket, GetObject, PutObject, etc.)
- **Folder Structure**: Pre-created folders (raw/, dlt_stage/, state/, marts/)

### Snowflake Integration
- SQL commands to create storage integration
- SQL commands to create stages (RAW_STAGE, DLT_S3_STAGE)

## Prerequisites

1. **AWS CLI configured** with appropriate permissions
2. **Terraform installed** (>= 1.0)
3. **Snowflake account** with ACCOUNTADMIN access

## Setup Instructions

### Step 1: Configure Terraform

```bash
# Navigate to infrastructure directory
cd infrastructure

# Copy example variables file
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values
nano terraform.tfvars
```

### Step 2: Initial Deployment (Bootstrap)

```bash
# Initialize Terraform
terraform init

# Plan the deployment
terraform plan

# Apply the changes
terraform apply
```

### Step 3: Configure Snowflake Integration

After the initial deployment, you need to complete the Snowflake side:

1. **Run the storage integration SQL** (from terraform output):
   ```sql
   -- Copy the SQL from: terraform output snowflake_storage_integration_sql
   ```

2. **Get the integration details**:
   ```sql
   DESC INTEGRATION opendata_stack_s3_integration;
   ```

3. **Update terraform.tfvars** with the values from step 2:
   - `STORAGE_AWS_IAM_USER_ARN` → `snowflake_account_id` and `snowflake_iam_user`
   - `STORAGE_AWS_EXTERNAL_ID` → `snowflake_external_id`

4. **Update the trust policy**:
   ```bash
   # Update variables and re-apply
   terraform apply
   ```

5. **Create Snowflake stages** (from terraform output):
   ```sql
   -- Copy the SQL from: terraform output snowflake_stage_sql
   ```

## Usage

### View Outputs
```bash
# Show all outputs
terraform output

# Show specific output
terraform output s3_bucket_name
terraform output iam_role_arn
```

### Update Configuration
```bash
# After changing variables
terraform plan
terraform apply
```

### Cleanup
```bash
# Destroy all resources (WARNING: This deletes everything!)
terraform destroy
```

## Important Notes

### Security
- The S3 bucket has public access blocked
- IAM role uses least-privilege permissions
- All resources are encrypted at rest

### Cost Optimization
- S3 bucket uses standard storage class
- Consider lifecycle policies for older data
- Monitor CloudWatch for usage patterns

### Troubleshooting

#### Common Issues

1. **"Access Denied" in Snowflake**
   - Verify IAM role trust policy is correct
   - Check that external ID matches exactly

2. **"Bucket already exists"**
   - S3 bucket names must be globally unique
   - Change the `project_name` variable

3. **"Invalid account ID"**
   - Ensure you're using the correct values from `DESC INTEGRATION`

#### Debug Commands
```bash
# Check AWS caller identity
aws sts get-caller-identity

# Verify S3 bucket
aws s3 ls s3://your-bucket-name

# Test IAM role assumption
aws sts assume-role --role-arn "arn:aws:iam::ACCOUNT:role/ROLE-NAME" --role-session-name "test"
```

## File Structure

```
infrastructure/
├── main.tf                    # Main Terraform configuration
├── variables.tf               # Input variables
├── outputs.tf                 # Output values
├── s3.tf                     # S3 bucket configuration
├── iam.tf                    # IAM roles and policies
├── terraform.tfvars.example  # Example variables
└── README.md                 # This file
```

## Environment Variables

Set these for automated deployments:

```bash
export AWS_REGION=eu-central-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Or use AWS profiles
export AWS_PROFILE=your-profile-name
```