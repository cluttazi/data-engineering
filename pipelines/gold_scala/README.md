# Gold Layer — Scala Spark Module

`DailyAccountAggregates`: per-account daily activity metrics with AML-style
screening flags, written as a Delta table consumed by analytics and the
observability stack.

```bash
./scripts/sbt test          # ScalaTest over in-memory frames
./scripts/sbt package       # thin jar (Spark/Delta are Provided)
make gold                   # spark-submit via the pyspark-shipped launcher
```

## Why a Scala module in a Python-first repo

Two honest reasons:

1. **JVM-side Spark is a real production skill** — performance-critical jobs,
   shared Scala libraries, and legacy estates all exist in Databricks shops;
   this module demonstrates the full loop (sbt build, `Provided` dependency
   discipline, ScalaTest with a shared session, spark-submit packaging).
2. **Cross-language platform contracts.** The job writes to the same metrics
   Delta table as the Python pipelines through a schema kept deliberately in
   lockstep (`MetricsWriter.scala` ↔ `observability/metrics/writer.py`) —
   the kind of boundary that actually bites in multi-language platforms.

## Design notes

- **Version lockstep**: `build.sbt` pins `spark-sql 4.1.1` / `delta-spark
  4.3.1` to match `pyproject.toml` exactly. `Provided` scope means the jar is
  thin and the runtime (pyspark's `spark-submit`) supplies Spark; a version
  drift here fails at runtime, so both files carry warnings.
- **Pure core**: `aggregate()` takes DataFrames in, returns a DataFrame —
  unit-testable without Delta or disk. IO lives only in `main`.
- **Overwrite, not MERGE**: gold is a deterministic full recompute from
  silver; reproducibility beats incrementality at this layer (the MERGE
  machinery lives in silver where it earns its complexity).
- **Screening flags**: `large_txn_flag` (threshold), `high_velocity_flag`
  (daily count), and `structuring_flag` — 3+ transactions in the
  85%-to-100%-of-threshold band in one day, the classic smurfing pattern.
  Thresholds are constants here; production would source them per currency.
- **Java 21**: forked tests carry the full `--add-opens` set Spark needs on
  modern JVMs (spark-submit injects these itself, sbt must not).

## sbt behind restrictive networks

`scripts/sbt` prefers a system sbt, else bootstraps `sbt-launch.jar` from
Maven Central — chosen because GitHub release downloads (the usual sbt
install path) are blocked in some proxied environments while Maven Central
rarely is.
