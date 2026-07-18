# Data Quality Framework

Declarative YAML suites executed by a small, typed PySpark runner.

```bash
make dq        # run all suites, print report, persist results
```

## Why a custom framework instead of Great Expectations

Considered and rejected for this platform (full analysis in ADR 005):
GE's value is its ecosystem, but its cost is a heavy dependency tree,
version churn, and a Spark integration that has historically lagged Spark
majors (risky on Spark 4.0/4.1). What the platform actually needs is small:
declarative suites reviewers can read, precise observed-value reporting, and
**results as data**. That is a few hundred lines of owned, typed, tested
code. The marts layer is covered by dbt tests; this framework covers the
Spark-side tables dbt can't reach.

## Design

- **Suites are YAML** (`suites/*.yaml`), validated into a pydantic
  discriminated union — unknown check types or missing params fail at load,
  not mid-run.
- **Severity model mirrors dbt**: `error` fails the run (exit 1), `warn`
  reports and continues. Referential-integrity checks are warns on purpose:
  orphans here trace to *quarantined upstream events* and belong in the
  incident report, not a hard stop.
- **Results go three places**: stdout (humans), `data/run/dq/*.json` (the
  incident agent's input), and the metrics Delta table (dashboard trends).
- **Freshness has a pinnable reference time** (`LAKEHOUSE_DQ_REFERENCE_TIME`)
  — the backfill-validation pattern, and what makes the deterministic
  logical-clock demo testable.

## Check types

`not_null`, `unique`, `accepted_values`, `row_count_between`, `freshness`,
`referential_integrity` — each ~10 lines in `checks.py`, each reporting the
observed value ("3 orphaned values"), because "check failed" is useless at
3am.
