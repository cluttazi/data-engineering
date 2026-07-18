# ADR 007: Keep Spark 4 ANSI mode ON; handle dirty data at the edges

Status: accepted · Date: 2026-07

## Context

Spark 4 enables ANSI SQL mode by default: invalid casts, arithmetic
overflow, and division by zero now raise errors instead of silently
producing nulls. A CDC platform ingests dirty data *by design* — the
tempting global fix is `spark.sql.ansi.enabled=false`, restoring the
permissive Spark 3 behavior everywhere.

## Decision

ANSI stays ON (the default is never overridden). Dirty data is handled at
explicit boundaries:

- envelope parsing uses `from_json` in PERMISSIVE mode with a
  corrupt-record column (bronze),
- payload typing parses against contract-compiled schemas permissively,
  then *enforces* nullability/enums as named violations (silver),
- any arithmetic on untrusted values uses `try_cast`/`try_divide`
  explicitly.

## Rationale

- **Silent nulls are the worst failure mode in finance.** The pre-ANSI
  behavior — a bad cast quietly becoming null and flowing into a balance
  aggregate — is precisely the class of bug that ends up in a regulator
  conversation. ANSI converts those into loud, located errors.
- **Turning it off is a global solution to a local problem.** Only the
  ingestion edges face untrusted bytes; everything downstream of contract
  enforcement operates on validated, typed data and *benefits* from strict
  semantics. Disabling ANSI everywhere to comfort two boundary layers
  inverts the risk profile.
- **The permissive edges are visible in code.** Every place lenient parsing
  happens is an explicit `PERMISSIVE`/`try_*` call — greppable, reviewed,
  and paired with quarantine routing so leniency never silently drops
  information (ADR 004).
- **Forward compatibility.** ANSI-on is Spark's direction and matches
  Databricks defaults on current runtimes; code written against it ports
  cleanly, while ANSI-off code accumulates latent breakage.

## Consequences

- Contributors must reach for `try_cast` (not `cast`) on untrusted inputs;
  a genuine bad cast in trusted code will fail the pipeline — which is the
  desired behavior, surfaced by the metrics table and the incident agent.
- PERMISSIVE `from_json` has one sharp corner: a non-object payload parses
  to an all-null struct rather than a null struct, so enforcement checks
  field-level nulls, not just struct nullity (covered by unit tests).
