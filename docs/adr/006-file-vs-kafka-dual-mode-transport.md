# ADR 006: Dual-mode CDC transport — one envelope, file and Kafka sources

Status: accepted · Date: 2026-07

## Context

The bronze layer must demonstrate real Structured Streaming from Kafka —
but the repo's hard requirement is that `make demo` runs end-to-end with
zero infrastructure, and CI runners (plus some sandboxed environments)
have no Docker daemon at all. Options:

1. **Kafka-only**, demo requires `make up` (Docker mandatory everywhere).
2. **File-only**, drop Kafka (no streaming-broker story at all).
3. **Dual transport behind one interface**: the simulator emits identical
   Debezium envelopes to NDJSON files (default) or Kafka; bronze builds its
   stream from either via config, normalized to one schema before any
   pipeline logic runs.

## Decision

Option 3. `pipelines/bronze/sources.py` exposes
`build_change_event_stream()` returning the normalized columns
`(entity, event_key, raw_value, source_ref)` from either transport;
`config/lakehouse.yaml` selects `file | kafka | auto` (auto = 1s TCP probe,
logged fallback). The Kafka connector jar is injected only in kafka mode so
file mode never touches the network.

## Rationale

- **The payload is the contract, not the pipe.** Everything that matters —
  envelope parsing, quarantine, contract enforcement, SCD2 — operates on
  the event bytes, which are identical across transports (the simulator
  serializes once; sinks only choose a destination). Swapping transport
  exercises zero downstream code paths, which is what makes the file mode
  an honest demo rather than a parallel implementation.
- **Demo and CI become hermetic** while the Kafka path stays code-complete
  and exercised whenever a broker exists (`make up`, then
  `LAKEHOUSE_SOURCE_MODE=kafka`). This is also the pattern's production
  value: local development against files, staging/production against the
  real broker, one codebase.
- **File mode reads `format("text")`, not `format("json")`** — a subtle
  load-bearing choice: the JSON reader nulls malformed lines, silently
  destroying the quarantine story; the text reader delivers bytes verbatim,
  keeping corruption handling in one explicit place for both transports.
- **Why not embedded/mock brokers in tests**: an in-process Kafka adds JVM
  fixture weight to prove transport plumbing that the normalized-envelope
  design makes trivially thin; the trade is covered by keeping the kafka
  branch small and declarative.

## Consequences

- The Kafka branch is exercised only when a broker is present (manual or
  compose-based), not in default CI — accepted, and mitigated by keeping
  that branch to ~20 declarative lines.
- `auto` mode's probe adds one second of latency when no broker exists;
  explicit `file`/`kafka` pins for anything scripted.
- A real Debezium deployment slots in by pointing the kafka source at the
  connector's topics — the simulator's envelope matches Debezium's payload
  format precisely for this reason.
