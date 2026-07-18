# Unity Catalog Governance-as-Code

Three artifacts, one principle — **governance metadata is derived from
sources of truth, never hand-maintained**:

| Artifact | Source of truth | Enforcement point |
|---|---|---|
| Privilege matrix | `grants.yaml` (this dir) | `platform/terraform/unity_catalog.tf` (`databricks_grants`) |
| PII column tags | `quality/contracts/*.yaml` (`pii` flags) | `generated/pii_tags.sql` (UC `SET TAGS`) |
| Access matrix doc | rendered from both | `generated/access_matrix.md` (audit evidence) |

Regenerate with `uv run python -m governance.unity_catalog.render`; CI
regenerates and fails on diff, so the committed artifacts cannot drift from
their sources.

## Why derive PII tags from contracts?

Every governance program dies the same death: the classification spreadsheet
goes stale. Here the *data contract* — the artifact producers and consumers
already review on every schema change — carries the `pii` / `pii_category`
flags, and everything else (UC tags, the masking expectations in marts, the
audit inventory) is generated from it. One review gate, no second list.

## Lineage notes

Lineage in this platform is structural rather than instrumented:

- **Bronze → silver**: silver rows carry `_run_id`; bronze events carry
  `run_id`, `source_ref` (file path / topic-partition-offset), and the
  Debezium `source.lsn` — a full chain from any silver version back to the
  exact raw line that produced it (and into `quarantine` for the rejects).
- **Silver → dbt marts**: dbt's own lineage graph (`dbt docs generate`),
  extended past the warehouse edge by the two exposures (dashboard, incident
  agent).
- **On Databricks**: Unity Catalog captures table/column lineage
  automatically for jobs on UC-enabled compute; the structural lineage above
  is what still works locally and in any non-UC engine.

## Local ↔ production mapping

Locally there is no metastore to enforce grants against — the demo's "access
control" is the PII-minimizing shape of the gold marts themselves (analysts'
only surface). In production the same YAML drives real `databricks_grants`;
the mapping is 1:1 by design so the local repo is a faithful rehearsal.
