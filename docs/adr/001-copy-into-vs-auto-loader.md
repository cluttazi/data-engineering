# ADR 001: COPY INTO semantics (explicit file ledger) over Auto Loader for batch reference feeds

Status: accepted · Date: 2026-07

## Context

The platform ingests two very different shapes of data: a high-frequency CDC
event stream (bronze streaming) and a long tail of low-frequency batch
reference feeds — branches, product catalogs, FX rates — dropped as files by
upstream teams. Databricks offers two idempotent file-ingestion mechanisms:

- **Auto Loader** (`cloudFiles`): incremental streaming file discovery with
  notification services or directory listing, RocksDB state in a checkpoint,
  schema inference/evolution built in. Designed for high-volume, continuous
  arrival.
- **COPY INTO**: a batch SQL statement that tracks loaded files in table
  metadata and loads each file exactly once. Designed for periodic,
  moderate-volume loads.

## Decision

Model the batch feeds on **COPY INTO semantics**, implemented locally as an
explicit **Delta file ledger** (`bronze/ops/file_ledger`): each run lists the
landing zone, subtracts already-ledgered paths, loads only the difference,
and appends per-file rows (path, dataset, row count, run, timestamp).

## Rationale

- **Fit to arrival pattern.** These feeds land a handful of files per day.
  Auto Loader's machinery (notification queues, streaming checkpoints,
  RocksDB state) buys nothing at this cadence and adds failure modes; the
  hard requirement is *exactly-once per file with an audit trail*, which is
  precisely what a ledger is.
- **Auditability is a feature, not a byproduct.** The ledger is a queryable
  table: when a regulator (or the incident agent) asks "which file did these
  rows come from and when was it loaded", the answer is one SELECT — with
  COPY INTO's hidden metadata or Auto Loader's checkpoint state it is not.
- **Resend behavior is explicit.** A byte-identical file under a new name
  (the classic upstream double-send, exercised by the batch generator) is
  loaded — new path, new file — and deduplicated downstream in silver by
  business key. Same behavior as COPY INTO; documented rather than
  accidental.
- **Local fidelity.** A ledger runs identically on a laptop and on
  Databricks. Auto Loader does not exist off-platform, so choosing it would
  have forced a local simulation of a proprietary mechanism — more code to
  demonstrate less.

## Consequences

- The CDC stream still uses real Structured Streaming with checkpoints
  (that arrival pattern *is* Auto Loader-shaped; on Databricks the file
  source would swap for `cloudFiles` with a one-line change in
  `sources.py`).
- Ledger listing is O(files in landing zone) per run — irrelevant at this
  cadence, and the natural migration at scale is Auto Loader, which this
  design mirrors closely enough to swap in.
- A file *modified in place* under the same name is not reloaded (same as
  COPY INTO's default); upstream contracts require new files for corrections.
