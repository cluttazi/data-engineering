# Compliance Mapping — FISC / FSA-Style Controls

How this platform's design choices map to the control expectations a
Japanese financial institution (FISC Security Guidelines, FSA supervisory
guidelines) — or any comparable regulator — would ask about. This is a
*design evidence* document: each control points at the mechanism in this
repo that implements it and where an auditor would look.

> Scope note: the demo runs on synthetic Faker data; no real personal or
> financial data ever enters this repository. The controls below describe
> the architecture as it would operate in production.

| # | Control expectation | Implementation | Evidence in repo |
|---|---|---|---|
| 1 | **Audit trail / traceability** — every derived record traceable to its source | Bronze stores raw envelopes immutably with `source_ref` + LSN; silver versions carry `_run_id`; quarantine keeps rejects with reasons | `pipelines/bronze/job.py`, `governance/unity_catalog/README.md` (lineage notes) |
| 2 | **Change history retention** — state changes must be reconstructible | SCD Type 2 historization: every entity version with `valid_from`/`valid_to`; deletes close chains rather than erase them (`is_deleted`) | `pipelines/silver/scd2.py`, ADR 002 |
| 3 | **Data retention & recoverability** | Delta time travel (logical), S3 versioning on the UC bucket (physical) — both provisioned in Terraform | `platform/terraform/unity_catalog.tf` |
| 4 | **Access control / least privilege** | Grants-as-code per layer: analysts read gold only; raw and PII-bearing layers restricted to engineering + service principals | `governance/unity_catalog/grants.yaml`, `generated/access_matrix.md` |
| 5 | **PII identification & classification** | Contract-embedded `pii`/`pii_category` flags; UC column tags generated from them; marts are PII-minimized by construction | `quality/contracts/definitions/`, `generated/pii_tags.sql`, `transform/.../dim_customers.sql` |
| 6 | **Data minimization / erasure** | CDC delete events (`op=d`) close SCD2 chains and drop records from current-state views while preserving the auditable history skeleton | `pipelines/silver/scd2.py`, `stg_loan_applications.sql` |
| 7 | **Input validation & anomaly isolation** | Contract enforcement at silver; quarantine-over-fail-fast so bad records are isolated, evidenced, and reprocessable | `pipelines/silver/enforcement.py`, ADR 004 |
| 8 | **Quality monitoring & alerting** | DQ suites with severities, results persisted as data; incident agent turns metric/DQ anomalies into actionable reports | `quality/expectations/`, `ai/agents/incident_reporter/` |
| 9 | **Operational monitoring** | Every pipeline step (both languages) writes to one metrics table: status, durations, row counts, failure messages | `observability/metrics/` |
| 10 | **Segregation of duties (pipeline identities)** | Distinct service principals per pipeline stage with layer-scoped grants (`svc-ingest` cannot touch silver) | `grants.yaml`, `platform/terraform/` |
| 11 | **Reproducibility / change management** | Infrastructure and grants in Terraform; contracts versioned with enforced compatibility rules; conventional-commit history; CI gates | `quality/contracts/compat.py`, `.github/workflows/` |
| 12 | **AML transaction monitoring (illustrative)** | Gold layer computes screening flags (large transaction, velocity, structuring) as first-class columns for downstream case management | `pipelines/gold_scala/` |

## What this is not

A control *framework* — production deployments need the organizational half
(policies, reviews, incident procedures, key management, penetration
testing). This document shows that the *technical* substrate those controls
need — immutability, historization, classification, least privilege,
observability — is designed in from the start rather than retrofitted.
