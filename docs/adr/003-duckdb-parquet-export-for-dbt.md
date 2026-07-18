# ADR 003: dbt on DuckDB over Parquet exports, not dbt-spark on Delta

Status: accepted · Date: 2026-07

## Context

The analytics layer (staging → marts, tests, docs, exposures) belongs in
dbt. Locally there are three ways to give dbt data:

1. **dbt-spark** against the same local Spark + Delta tables the pipelines
   write.
2. **dbt-duckdb reading Delta directly** via DuckDB's `delta` extension
   (delta-kernel-rs).
3. **dbt-duckdb over Parquet snapshots** exported from silver by an explicit
   pipeline step.

## Decision

Option 3: `pipelines/silver/export_for_dbt.py` snapshots silver (and batch
reference tables) to Parquet directories; dbt sources use DuckDB
`read_parquet` with an env-overridable root.

## Rationale

- **Against dbt-spark locally**: every dbt invocation pays JVM/session
  startup (tens of seconds before the first model), turning the tightest
  iteration loop in the stack into the slowest. It also couples the
  analytics layer to the Spark runtime — the wrong dependency direction:
  marts should depend on *published data*, not on the engine that produced
  it.
- **Against the DuckDB delta extension**: it downloads at runtime (breaks
  offline/proxied environments) and reads Delta through delta-kernel-rs,
  whose coverage of newer Delta 4 table features (deletion vectors from
  MERGE-heavy SCD2 tables, column mapping from schema evolution — both of
  which this platform *actually produces*) is exactly the kind of edge to
  not bet a demo or a production handoff on.
- **For the explicit export**: it mirrors the real pattern — curated
  snapshots published to a consumption layer with a defined interface —
  and it makes the dbt project *hermetic*: CI builds it from tiny committed
  fixtures (`scripts/make_dbt_fixtures.py`) with no Spark, no JVM, in
  seconds. The export step is also the natural seam where a production
  deployment would swap in UC table reads or a warehouse share.

## Consequences

- The export is a copy: storage duplication (trivial at this scale) and
  staleness bounded by pipeline cadence — the export runs inside the same
  orchestrated flow, so marts are exactly as fresh as silver's last run.
- dbt tests validate the *published* snapshot, not the live Delta table;
  the DQ framework covers the Delta side. This split is intentional: each
  tool tests the surface its consumers actually read.
- On Databricks the same dbt project retargets `dbt-databricks` against UC
  tables by swapping profile + sources; models are engine-portable SQL by
  construction (the `qualify` clauses are the one DuckDB-ism, called out
  in the project README).
