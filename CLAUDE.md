# CLAUDE.md — agent instructions for lakehouse-platform

## What this repo is

A local-first lakehouse on a synthetic retail-banking domain: CDC ingestion
(bronze) → contract-enforced SCD2 (silver) → Scala aggregates (gold) → dbt
marts, with a custom DQ framework, governance-as-code, and an offline RAG +
incident-agent layer. Everything runs without Docker or cloud credentials.

## Commands

```bash
uv sync --group dev          # environment (Python 3.11, Java 21 required)
make demo                    # full end-to-end run with summary
make cdc bronze silver ...   # individual steps (see make help)
make lint                    # ruff check + format check + mypy (all must pass)
make test                    # pytest; spark-marked tests need the JVM
./scripts/sbt test           # Scala module tests (bootstraps sbt if absent)
make dbt                     # dbt build over exported silver data
```

## Hard rules

- **Version lockstep**: `pyspark==4.1.1` + `delta-spark==4.3.1` in
  `pyproject.toml` MUST match `sparkVersion`/`deltaVersion` in
  `pipelines/gold_scala/build.sbt` and the `--packages` coordinate in
  `scripts/run_gold.sh`. Java 21 requires Spark 4.x — never downgrade to
  the 3.5 line.
- **ANSI mode stays on** (ADR 007). Use `try_cast` on untrusted data; never
  set `spark.sql.ansi.enabled=false`.
- **Contracts are the source of truth** (`quality/contracts/definitions`).
  Schema changes go through a new contract version that passes
  `uv run python -m quality.contracts.compat`. PII flags there drive the
  generated governance artifacts — after touching contracts or
  `governance/unity_catalog/grants.yaml`, run
  `uv run python -m governance.unity_catalog.render` and commit the diff
  (CI fails on drift).
- **Metrics schema is cross-language**: `observability/metrics/writer.py`
  and `pipelines/gold_scala/.../MetricsWriter.scala` must stay in lockstep.
- **No real data, ever.** All data is Faker-generated under seeds; keep new
  generators deterministic (seeded RNG + logical clock, no wall-time in
  payloads).
- `data/` is disposable (`make clean`); never commit it. Stale streaming
  checkpoints after manual `data/` surgery cause silent 0-row bronze runs —
  clean fully instead.

## Conventions

- Conventional commits (`feat(bronze): ...`, `docs: ...`).
- Type hints everywhere; mypy strict is a gate. New untyped third-party
  deps get a targeted override in `pyproject.toml`, never a blanket ignore.
- Every module README explains *why*, not just how; significant design
  trade-offs get an ADR in `docs/adr/`.
- Tests: unit tests colocated by module under `tests/unit/…`; anything
  starting a SparkSession gets `@pytest.mark.spark` (or module-level
  `pytestmark`); cross-layer flows live in `tests/integration/`.

## Architecture pointers

Read `docs/architecture.md` first, then the ADR index in `docs/adr/`. The
one intentional cross-module dependency is `pipelines/common` (config +
session factory) — everything else communicates through data (Delta tables,
Parquet exports, JSON results), not imports.
