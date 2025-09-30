# IAM Role for Snowflake Storage Integration
resource "aws_iam_role" "snowflake_role" {
  name = "${var.project_name}-snowflake-role-${var.environment}"

  assume_role_policy = var.snowflake_aws_account_id != null ? jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.snowflake_aws_account_id}:user/${var.snowflake_aws_user}"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      }
    ]
  }) : jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Deny"
        Principal = {
          AWS = "*"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "${var.project_name}-snowflake-role-${var.environment}"
    Description = "IAM role for Snowflake storage integration"
  }
}

# IAM Policy for S3 Access
resource "aws_iam_policy" "snowflake_s3_policy" {
  name        = "${var.project_name}-snowflake-s3-policy-${var.environment}"
  description = "Policy for Snowflake to access S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:DeleteObject",
          "s3:DeleteObjectVersion"
        ]
        Resource = [
          "${aws_s3_bucket.snowflake_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.snowflake_data.arn
        ]
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "raw/*",
              "dlt_stage/*",
              "state/*",
              "marts/*"
            ]
          }
        }
      }
    ]
  })
}

# Attach the policy to the role
resource "aws_iam_role_policy_attachment" "snowflake_s3_policy_attachment" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_policy.arn
}

# Update IAM role trust policy with Snowflake integration details
resource "null_resource" "update_iam_trust_policy" {
  count = var.snowflake_aws_account_id != null ? 0 : 1

  # Trigger update when storage integration is created
  triggers = {
    storage_integration_id = snowflake_storage_integration.s3_integration.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      # Get Snowflake storage integration details
      STORAGE_AWS_IAM_USER_ARN="${snowflake_storage_integration.s3_integration.storage_aws_iam_user_arn}"
      STORAGE_AWS_EXTERNAL_ID="${snowflake_storage_integration.s3_integration.storage_aws_external_id}"

      # Extract AWS account ID and user name from ARN
      AWS_ACCOUNT_ID=$(echo $STORAGE_AWS_IAM_USER_ARN | cut -d':' -f5)
      AWS_USER=$(echo $STORAGE_AWS_IAM_USER_ARN | cut -d'/' -f2)

      # Update IAM role trust policy
      aws iam update-assume-role-policy \
        --role-name ${aws_iam_role.snowflake_role.name} \
        --policy-document "{
          \"Version\": \"2012-10-17\",
          \"Statement\": [
            {
              \"Effect\": \"Allow\",
              \"Principal\": {
                \"AWS\": \"$STORAGE_AWS_IAM_USER_ARN\"
              },
              \"Action\": \"sts:AssumeRole\",
              \"Condition\": {
                \"StringEquals\": {
                  \"sts:ExternalId\": \"$STORAGE_AWS_EXTERNAL_ID\"
                }
              }
            }
          ]
        }"

      echo "✅ IAM trust policy updated successfully!"
      echo "   AWS Account: $AWS_ACCOUNT_ID"
      echo "   AWS User: $AWS_USER"
      echo "   External ID: $STORAGE_AWS_EXTERNAL_ID"
    EOT
  }

  depends_on = [
    aws_iam_role.snowflake_role,
    snowflake_storage_integration.s3_integration
  ]
}
