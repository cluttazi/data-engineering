---
name: dq-auditor
description: Run and interpret the data-quality suites, trace warns/failures to root cause (quarantine, contracts, pipeline bugs), and propose the right fix. Use when asked to investigate data quality, DQ failures, orphans, or quarantine contents.
---

# DQ Auditor

## Run

```bash
make dq                      # all suites; exit 1 only on error-severity failures
cat data/run/dq/*.json       # machine-readable results (what the incident agent reads)
make agent                   # incident report with findings + probable causes
```

## Interpretation playbook

Work root-cause-first; the check that fired is a symptom, not the problem.

| Symptom | First look | Usual cause → right fix |
|---|---|---|
| `referential_integrity` orphans (warn) | `bronze/quarantine` for the missing keys' insert events | Corrupt upstream events quarantined → fix producer / replay; NOT a defect in the checked table. Do not "fix" by deleting orphans. |
| `null_violation` / `enum_violation` rows in `silver/quarantine` | `violations` column names the exact rule | Producer drift → contract conversation; if the change is legitimate, evolve the contract (new version + compat check), never patch data in place |
| `freshness` warn | Is `LAKEHOUSE_DQ_REFERENCE_TIME` pinned? Demo data uses a 2026-01 logical clock, so wall-clock freshness is expectedly stale | Pin the reference time for backfill validation; only investigate if a *pinned* check fails |
| `unique` failure on a silver table | SCD2 filter in the suite (`is_current = true`)? | Missing filter = false positive; with filter = real merge bug — check `scd2.py` chain invariants via `tests/integration` |
| Quarantine spike (agent finding) | `error_reason` distribution in `bronze/quarantine` | One reason dominating = upstream format change; mixed = transport corruption |
| `row_count_between` = 0 rows | Metrics table: did bronze write 0 rows? | Stale checkpoint after manual `data/` surgery → `make clean` and re-run, don't delete checkpoints selectively |

## When proposing new checks

- Mirror the contract: if a rule matters, it should usually exist in *both*
  the contract (silver enforcement, row-level) and a suite (table-level).
- Choose severity by blast radius, not by confidence: `error` blocks
  promotion, `warn` feeds the incident agent. RI stays `warn` by policy
  (ADR 004 explains why).
- Every check must report an *observed value* string — follow the existing
  `checks.py` pattern ("3 orphaned values", never just pass/fail).
- Add a unit test in `tests/unit/quality/test_dq_checks.py` with a crafted
  frame that fails the check.
