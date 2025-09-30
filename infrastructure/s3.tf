# S3 Bucket for Snowflake Data Loading
resource "aws_s3_bucket" "snowflake_data" {
  bucket = local.bucket_name
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "snowflake_data" {
  bucket = aws_s3_bucket.snowflake_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "snowflake_data" {
  bucket = aws_s3_bucket.snowflake_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# S3 Bucket public access block
resource "aws_s3_bucket_public_access_block" "snowflake_data" {
  bucket = aws_s3_bucket.snowflake_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create standard folder structure
resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.snowflake_data.id
  key    = "raw/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "dlt_stage_folder" {
  bucket = aws_s3_bucket.snowflake_data.id
  key    = "dlt_stage/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "state_folder" {
  bucket = aws_s3_bucket.snowflake_data.id
  key    = "state/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "marts_folder" {
  bucket = aws_s3_bucket.snowflake_data.id
  key    = "marts/"
  content_type = "application/x-directory"
}