# Terraform — Databricks Workspace + Unity Catalog on AWS

> **⚠️ Deploy-ready, never applied here.**
> This module is the production target of the repo. The local demo runs
> entirely on the file-based lakehouse under `data/` — nothing in this
> directory is required (or invoked) by `make demo`. CI runs
> `terraform validate` against it; `terraform apply` is a deliberate,
> credentialed act by a human with an AWS account and a Databricks account.

## What it provisions

| Layer | Resources |
|---|---|
| Workspace | E2 workspace, root S3 bucket + Databricks bucket policy, cross-account IAM role (`workspace.tf`) |
| Unity Catalog | Metastore, workspace assignment, storage credential (IAM role), external location, versioned S3 bucket (`unity_catalog.tf`) |
| Medallion layout | Catalogs `bronze` / `silver` / `gold` with per-layer schemas (`banking`, `quarantine`, `ops`, `observability`) |
| Access control | `databricks_grants` implementing least privilege: engineers modify bronze/silver, analysts read gold only |

## Why it looks the way it does

- **Grants-as-code, mirrored in governance.** The same privilege matrix is
  declared in `governance/unity_catalog/grants.yaml` and rendered into the
  human-readable audit matrix. Terraform is the enforcement point; YAML is
  the review/evidence artifact. Drift between them is a review failure.
- **Analysts never touch bronze or silver.** Raw CDC payloads and
  pre-masking PII live there; the gold layer is the only supported
  consumption surface. This maps directly to the FISC/FSA access-control
  expectations documented in `governance/compliance/`.
- **S3 versioning on the UC bucket** backs the recoverability control:
  Delta time travel covers logical history, object versioning covers
  physical-deletion accidents.
- **Two provider aliases** (`account`, `workspace`) because workspace
  provisioning and catalog management authenticate against different planes;
  keeping them separate makes the bootstrap order explicit.

## Mapping local → cloud

| Local demo | This module |
|---|---|
| `data/lakehouse/bronze|silver|gold` directories | UC catalogs on the external location |
| Directory naming conventions | Catalog/schema grants + isolation mode |
| MinIO (docker-compose) | The `-uc` S3 bucket |
| `config/lakehouse.yaml` entity list | Schemas under each catalog |

## Usage (in a real deployment)

```bash
export DATABRICKS_CLIENT_ID=...      # account-level OAuth M2M service principal
export DATABRICKS_CLIENT_SECRET=...
export AWS_PROFILE=...

terraform init
terraform plan -var databricks_account_id=<account-id>
terraform apply -var databricks_account_id=<account-id>
```

State should live in a remote backend (S3 + DynamoDB lock) in any shared
environment; the backend block is intentionally omitted so `validate`
works without infrastructure and each deployment chooses its own.
