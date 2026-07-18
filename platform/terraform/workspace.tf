# Databricks E2 workspace on AWS: root bucket, cross-account IAM role,
# and the workspace itself. Account-scoped resources use the `account`
# provider alias.

data "aws_caller_identity" "current" {}

data "databricks_aws_assume_role_policy" "this" {
  provider    = databricks.account
  external_id = var.databricks_account_id
}

data "databricks_aws_crossaccount_policy" "this" {
  provider = databricks.account
}

resource "aws_iam_role" "cross_account" {
  name               = "${var.prefix}-crossaccount"
  assume_role_policy = data.databricks_aws_assume_role_policy.this.json
}

resource "aws_iam_role_policy" "cross_account" {
  name   = "${var.prefix}-crossaccount-policy"
  role   = aws_iam_role.cross_account.id
  policy = data.databricks_aws_crossaccount_policy.this.json
}

resource "aws_s3_bucket" "root_storage" {
  bucket        = "${var.prefix}-${var.environment}-root"
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "root_storage" {
  bucket = aws_s3_bucket.root_storage.id
  versioning_configuration {
    # Databricks manages DBFS root content; versioning off per Databricks guidance.
    status = "Suspended"
  }
}

resource "aws_s3_bucket_public_access_block" "root_storage" {
  bucket                  = aws_s3_bucket.root_storage.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "databricks_aws_bucket_policy" "root_storage" {
  provider = databricks.account
  bucket   = aws_s3_bucket.root_storage.bucket
}

resource "aws_s3_bucket_policy" "root_storage" {
  bucket = aws_s3_bucket.root_storage.id
  policy = data.databricks_aws_bucket_policy.root_storage.json
}

resource "databricks_mws_credentials" "this" {
  provider         = databricks.account
  credentials_name = "${var.prefix}-${var.environment}-credentials"
  role_arn         = aws_iam_role.cross_account.arn
}

resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.account
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${var.prefix}-${var.environment}-storage"
  bucket_name                = aws_s3_bucket.root_storage.bucket
}

resource "databricks_mws_workspaces" "this" {
  provider                 = databricks.account
  account_id               = var.databricks_account_id
  workspace_name           = "${var.prefix}-${var.environment}"
  aws_region               = var.aws_region
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  pricing_tier             = "PREMIUM" # Unity Catalog requires Premium or above
}
