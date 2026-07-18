variable "aws_region" {
  description = "AWS region for the workspace root bucket and Unity Catalog storage."
  type        = string
  default     = "ap-northeast-1"
}

variable "databricks_account_id" {
  description = "Databricks account ID (from the account console)."
  type        = string
}

variable "prefix" {
  description = "Resource name prefix; keeps all provisioned resources greppable."
  type        = string
  default     = "lakehouse-platform"
}

variable "workspace_admins" {
  description = "Workspace admin principals (user emails or SP application IDs)."
  type        = list(string)
  default     = []
}

variable "data_engineers_group" {
  description = "Account-level group granted read/write on silver, read on bronze."
  type        = string
  default     = "data-engineers"
}

variable "analysts_group" {
  description = "Account-level group granted read-only access to gold."
  type        = string
  default     = "analysts"
}

variable "environment" {
  description = "Deployment environment tag (dev/stg/prod)."
  type        = string
  default     = "dev"
}
