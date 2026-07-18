# dbt Project — Analytics Layer on DuckDB

Staging → marts over Parquet snapshots exported from silver, with tests,
exposures, and generated docs.

```bash
make dbt          # dbt build (models + tests)
uv run dbt docs generate --project-dir transform/dbt_project --profiles-dir transform/dbt_project
```

## Why dbt-on-DuckDB here, when the pipelines are Spark

Deliberate separation of concerns (full trade-off analysis in
`docs/adr/003-duckdb-parquet-export-for-dbt.md`):

- **Spark owns ingestion and historization** (streaming, CDC merge, contracts)
  where distributed execution and Delta ACID matter.
- **dbt owns the analytics layer** where SQL, tests-as-docs, lineage, and
  exposures matter. DuckDB makes that layer instant and hermetic locally —
  the dbt CI job needs no Spark, no JVM, just committed fixture Parquet.
- The hand-off is an explicit Parquet export (`pipelines/silver/export_for_dbt.py`),
  mirroring the production pattern of publishing curated snapshots to a
  consumption layer rather than letting BI tools read operational tables.

## Layout

| Layer | Models | Notes |
|---|---|---|
| sources | `silver.*` | `read_parquet` external locations, env-overridable for fixtures |
| staging | `stg_*` | current-version views over SCD2 history; batch-feed dedup (`qualify row_number()`) |
| marts | `dim_customers`, `fct_transactions`, `mart_loan_funnel` | PII-minimized dimension, JPY-normalized fact, funnel with approval rates |

## Notable choices

- **PII stops at staging.** `dim_customers` carries no direct identifiers —
  the dbt layer mirrors the Unity Catalog grant model (analysts never read
  silver). Governance by construction, not by masking afterthought.
- **Referential-integrity tests are `warn` at staging**: an orphaned
  account/transaction here almost always traces to a *quarantined upstream
  CDC event* — that's a data-quality signal (surfaced by the DQ suite and
  the incident agent), not a reason to block the analytics build. Key
  uniqueness/nullability remain hard errors.
- **Exposures** declare the two real consumers (observability dashboard,
  incident agent) so `dbt docs` lineage reaches past the warehouse edge.
