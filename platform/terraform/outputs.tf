output "workspace_url" {
  description = "URL of the provisioned Databricks workspace."
  value       = databricks_mws_workspaces.this.workspace_url
}

output "metastore_id" {
  description = "Unity Catalog metastore ID."
  value       = databricks_metastore.this.id
}

output "catalog_names" {
  description = "Medallion catalogs created in Unity Catalog."
  value       = [for c in databricks_catalog.medallion : c.name]
}

output "unity_catalog_bucket" {
  description = "S3 bucket backing Unity Catalog managed storage."
  value       = aws_s3_bucket.unity_catalog.bucket
}
