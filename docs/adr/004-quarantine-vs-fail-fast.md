# ADR 004: Quarantine tables over fail-fast for bad records

Status: accepted · Date: 2026-07

## Context

CDC feeds and file drops carry garbage: truncated JSON, type-mangled
fields, contract violations. Two philosophies:

- **Fail fast**: any bad record stops the pipeline; nothing moves until a
  human intervenes.
- **Quarantine**: route bad records to dedicated tables with reasons;
  healthy data keeps flowing.

## Decision

Quarantine at both validation layers, with different granularity:

- **Bronze**: structurally broken events (unparseable, envelope-incomplete)
  → `bronze/quarantine` with `error_reason`, raw line, source reference.
- **Silver**: contract violations (nullability, enums, unparseable payload)
  → `silver/quarantine` with the *named* violated rules per row.

Failure remains the right response for *systemic* errors: a missing table,
a broken checkpoint, or a schema that can't be reconciled still fails the
step (and the metrics table + incident agent report it).

## Rationale

- **Blast-radius control.** In the demo, 2% of events are deliberately
  corrupt. Fail-fast would convert 2% bad data into 100% data unavailability
  — in a bank, one malformed message stopping the transaction feed is
  itself an incident. Bad *records* are data problems; bad *systems* are
  pipeline problems; the two deserve different responses.
- **Evidence beats logs.** Quarantine rows carry the raw payload and the
  reason — queryable, retained (90 days per the retention policy),
  attachable to an incident. A fail-fast stack trace tells you the first
  bad record; the quarantine table shows you all of them and their
  distribution (the incident agent's spike detector reads exactly this).
- **Reprocessing is a query away.** Fixed upstream? Select from quarantine,
  replay through enforcement. Fail-fast offers no such path — the data
  died in a log line.
- **Downstream visibility is preserved, not hidden.** The known consequence
  of quarantining is silent partial data. Mitigations are built in: bronze
  counts quarantined rows in metrics, DQ referential-integrity checks
  surface the resulting orphans as warns, and the incident agent connects
  the two ("orphans usually trace to quarantined upstream events"). The
  demo shows this exact chain live.

## Consequences

- Quarantine tables need retention discipline (90 days, per policy) or
  they become a swamp.
- Consumers must accept eventually-repaired completeness rather than
  all-or-nothing snapshots — the correct trade in a streaming platform,
  and the one every mature CDC deployment makes.
