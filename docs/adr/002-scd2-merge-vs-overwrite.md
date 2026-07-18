# ADR 002: SCD Type 2 via MERGE INTO (recompute-then-merge) over overwrite or append-only

Status: accepted · Date: 2026-07

## Context

Silver must historize mutable entities (customers, accounts, loan
applications) from CDC events carrying before/after images. Candidate
strategies:

1. **Overwrite current state** — keep only the latest version per key.
2. **Append-only event log** — store every change, push versioning to
   consumers.
3. **SCD Type 2 maintained by MERGE INTO** — explicit version rows with
   `valid_from`/`valid_to`/`is_current`, updated transactionally.

And within SCD2, two maintenance styles: incremental (merge only new events
against open versions) vs. **recompute-then-merge** (rebuild version chains
from full history, merge the result idempotently).

## Decision

SCD Type 2, maintained by **recomputing version chains from bronze history
and MERGEing on `(business_key, valid_from)`** — update closure columns on
match, insert new versions otherwise. Deletes close chains (`is_deleted`)
rather than removing rows. Append-only entities (transactions) use
insert-if-absent MERGE.

## Rationale

- **Overwrite destroys the product.** In a regulated banking context the
  history *is* the requirement (FISC-style reconstruction expectations —
  see the compliance matrix). "What was this customer's risk rating when
  the loan was approved?" must be answerable; overwrite makes it
  unanswerable and no cheaper to run.
- **Raw event log alone pushes complexity to every consumer.** Each analyst
  join against "state as of T" re-implements versioning, differently, with
  different bugs. SCD2 does it once, tested, in the platform.
- **Why MERGE and not blind append of versions**: MERGE is what makes the
  operation *idempotent* — replaying the same bronze events is a no-op, a
  crashed run can simply re-run, and late events repair `valid_to` on
  previously-open versions in place. Idempotency is the property that makes
  operations boring, and boring is the goal.
- **Why recompute-then-merge locally**: it is O(history) but removes an
  entire class of incremental-state bugs (open-version bookkeeping,
  out-of-order arrivals across batch boundaries) and makes the logic
  testable as one pure transformation. At demo/reference scale the cost is
  milliseconds. The MERGE statement is unchanged when narrowing the
  recompute window to touched keys at production scale — the migration
  path is a WHERE clause, not a redesign.

## Consequences

- Silver tables carry version metadata columns that staging views must
  filter (`is_current AND NOT is_deleted`) — done once in dbt staging.
- Full-history recompute would need the touched-keys optimization beyond
  ~10⁷ events per entity; documented in `pipelines/silver/scd2.py`.
- Delete events produce closed chains, satisfying data-minimization
  requests while preserving the audit skeleton — this dual requirement is
  exactly what SCD2 handles gracefully and the alternatives do not.
