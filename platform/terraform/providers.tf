# Two Databricks provider scopes:
#   * account  — Databricks account console (workspace provisioning, metastore)
#   * workspace — the provisioned workspace (catalogs, schemas, grants)
#
# Authentication is intentionally NOT hard-coded: use environment variables
# (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET for OAuth M2M, and the
# standard AWS credential chain). Nothing in this module is ever applied from
# the local demo environment — see README.md.

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project    = "lakehouse-platform"
      managed_by = "terraform"
    }
  }
}

provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

provider "databricks" {
  alias = "workspace"
  host  = databricks_mws_workspaces.this.workspace_url
}
