# Unity Catalog: metastore, storage credential, external location, and the
# Medallion catalog/schema layout with grants-as-code.
#
# The privilege matrix lives in governance/unity_catalog/grants.yaml and is
# mirrored here; keep the two in sync (the governance module renders the YAML
# into the human-readable access matrix used for audit evidence).

locals {
  catalogs = {
    bronze = "Raw immutable ingestion layer. Append-only, quarantine included."
    silver = "Validated, deduplicated, SCD2-historized entities."
    gold   = "Aggregated business-level marts consumed by analytics."
  }

  # schema layout per catalog: banking domain + operational schemas
  schemas = {
    bronze = ["banking", "quarantine", "ops"]
    silver = ["banking", "quarantine"]
    gold   = ["banking", "observability"]
  }
}

resource "aws_s3_bucket" "unity_catalog" {
  bucket        = "${var.prefix}-${var.environment}-uc"
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "unity_catalog" {
  bucket = aws_s3_bucket.unity_catalog.id
  versioning_configuration {
    # Versioning supports the FISC-style recoverability control (see
    # governance/compliance/README.md); Delta time travel covers logical
    # history, S3 versioning covers accidental object deletion.
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "unity_catalog" {
  bucket                  = aws_s3_bucket.unity_catalog.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  provider       = databricks.account
  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = "${var.prefix}-uc-access"
  external_id    = var.databricks_account_id
}

data "databricks_aws_unity_catalog_policy" "this" {
  provider       = databricks.account
  aws_account_id = data.aws_caller_identity.current.account_id
  bucket_name    = aws_s3_bucket.unity_catalog.bucket
  role_name      = "${var.prefix}-uc-access"
}

resource "aws_iam_role" "unity_catalog" {
  name               = "${var.prefix}-uc-access"
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
}

resource "aws_iam_role_policy" "unity_catalog" {
  name   = "${var.prefix}-uc-access-policy"
  role   = aws_iam_role.unity_catalog.id
  policy = data.databricks_aws_unity_catalog_policy.this.json
}

resource "databricks_metastore" "this" {
  provider      = databricks.account
  name          = "${var.prefix}-${var.environment}"
  region        = var.aws_region
  storage_root  = "s3://${aws_s3_bucket.unity_catalog.bucket}/metastore"
  force_destroy = false
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  metastore_id = databricks_metastore.this.id
  workspace_id = databricks_mws_workspaces.this.workspace_id
}

resource "databricks_storage_credential" "this" {
  provider = databricks.workspace
  name     = "${var.prefix}-uc-credential"
  comment  = "IAM role used by Unity Catalog to access the lakehouse bucket"

  aws_iam_role {
    role_arn = aws_iam_role.unity_catalog.arn
  }

  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_external_location" "lakehouse" {
  provider        = databricks.workspace
  name            = "${var.prefix}-lakehouse"
  url             = "s3://${aws_s3_bucket.unity_catalog.bucket}/lakehouse"
  credential_name = databricks_storage_credential.this.name
  comment         = "Root external location for the medallion catalogs"
}

resource "databricks_catalog" "medallion" {
  provider = databricks.workspace
  for_each = local.catalogs

  name           = each.key
  comment        = each.value
  storage_root   = "${databricks_external_location.lakehouse.url}/${each.key}"
  isolation_mode = "ISOLATED"

  properties = {
    layer       = each.key
    environment = var.environment
  }
}

resource "databricks_schema" "medallion" {
  provider = databricks.workspace
  for_each = {
    for pair in flatten([
      for catalog, schemas in local.schemas : [
        for schema in schemas : { catalog = catalog, schema = schema }
      ]
    ]) : "${pair.catalog}.${pair.schema}" => pair
  }

  catalog_name = databricks_catalog.medallion[each.value.catalog].name
  name         = each.value.schema
  comment      = "Managed by terraform — see governance/unity_catalog for the access matrix"
}

# Grants follow least privilege:
#   data engineers: full modify on bronze/silver, read gold
#   analysts:       read gold only — never raw or PII-bearing silver
resource "databricks_grants" "bronze" {
  provider = databricks.workspace
  catalog  = databricks_catalog.medallion["bronze"].name

  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}

resource "databricks_grants" "silver" {
  provider = databricks.workspace
  catalog  = databricks_catalog.medallion["silver"].name

  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
}

resource "databricks_grants" "gold" {
  provider = databricks.workspace
  catalog  = databricks_catalog.medallion["gold"].name

  grant {
    principal  = var.data_engineers_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }

  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}
