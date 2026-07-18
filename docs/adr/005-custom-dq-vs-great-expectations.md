# ADR 005: Custom PySpark DQ framework + dbt tests, not Great Expectations

Status: accepted · Date: 2026-07

## Context

The platform needs declarative quality checks on Spark-side tables (silver,
gold) with results that feed the dashboard and the incident agent. Great
Expectations is the incumbent brand; the alternative is a small owned
framework plus dbt's native tests for the marts layer.

## Decision

A custom framework (`quality/expectations`: ~400 lines — pydantic suite
models, six check implementations, a runner) for Delta tables, and dbt
generic/singular tests for the analytics layer.

## Rationale

- **Dependency risk vs. value actually used.** GE brings a large dependency
  tree and a history of breaking API rewrites (v0.x → 1.x churn), and its
  Spark execution engine has lagged Spark majors — a real hazard on a
  Spark 4.1 platform pinned for Java 21. The features the platform would
  actually use (six check types, YAML definitions, machine-readable
  results) are a fraction of GE's surface.
- **Results-as-data is the real requirement.** The consumers here are not
  GE's Data Docs but the metrics Delta table, the incident agent's JSON
  input, and the dashboard. A framework whose *output contract* is designed
  for those consumers is simpler than adapting GE's result objects.
- **The severity model matters more than the check library.** The
  dbt-style `error`/`warn` split — with referential integrity deliberately
  `warn` because orphans trace to quarantined upstream events (ADR 004) —
  is the operationally meaningful part, and it's ~10 lines to own.
- **dbt already covers the marts.** Where dbt runs, its tests are the
  native idiom (schema-adjacent, lineage-aware, docs-integrated); no reason
  to duplicate.
- **Honest counterpoint**: GE's name recognition and its profiling/docs UI
  are lost. For a platform whose DQ results are consumed by machines and a
  dashboard — not by a data-catalog audience — that trade is favorable.
  Owned code also means owned maintenance; bounded by keeping the framework
  deliberately small.

## Consequences

- New check types are code changes (small, tested ones) rather than
  configuration of a vendor library.
- The YAML suite format is project-specific; mitigated by pydantic
  validation with precise errors and a format that reads like dbt tests.
- If the platform later needs profiling or expectation auto-generation,
  GE (or its lighter successors) can be adopted *for that layer* without
  touching the severity/reporting contract.
