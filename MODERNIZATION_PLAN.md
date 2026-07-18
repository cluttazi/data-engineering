# Modernization audit plan

Audit date: 2026-07-18 · branch `claude/repo-modernization-audit-6se2sn`
Scope: audit + light-touch pass. No architectural changes; all CLAUDE.md
hard rules respected (version lockstep, ANSI on, contracts as source of
truth, cross-language metrics schema, deterministic generators).

## Gate results (baseline, before changes)

| Gate | Command | Result |
|---|---|---|
| Env sync | `uv sync --group dev` | PASS |
| Lint | `make lint` (ruff check + format check + mypy strict) | PASS (78 files, 0 issues) |
| Python tests | `make test` | PASS — 71/71 in ~152s (unit + spark + integration) |
| Scala tests | `./scripts/sbt test` | PASS — 6/6 ScalaTest |
| Contract compat | `uv run python -m quality.contracts.compat` | PASS (customers v1→v2 compatible) |
| Governance drift | `uv run python -m governance.unity_catalog.render` + `git diff` | PASS (no drift) |
| End-to-end demo | `make demo` | **FAIL** — `gold_scala` step (see finding F1); dbt_build + data_quality skipped as a consequence; all other steps ok |

## Findings

### F1 (bug): `scripts/run_gold.sh` passes `-batch` to the sbt wrapper

When the gold jar is absent, `run_gold.sh` runs `./scripts/sbt -batch package`.
`scripts/sbt` execs a system `sbt` when present; in environments where that
`sbt` is a raw `sbt-launch.jar` shim (as here: sbt-launch 1.11.7), `-batch`
is not a valid launcher/command argument — the official `sbt` runner script's
flag only — and the build aborts with `[error] Expected '--' … -batch`,
failing the demo's gold step and skipping dbt/DQ downstream.

Fix: drop `-batch`. When sbt is given explicit commands it runs
non-interactively and exits anyway, so the flag adds nothing on the official
runner and breaks the raw-launcher path. One-line script fix; verified by
re-running `make demo` from a clean jar state (the demo exercises the
package-then-submit path end to end).

### F2 (info): dependency inventory vs latest stable (cutoff Jan 2026)

Runtime (pins are current or intentionally locked):

- `pyspark==4.1.1` + `delta-spark==4.3.1` — verified pair, locked with
  `pipelines/gold_scala/build.sbt` and `scripts/run_gold.sh` (CLAUDE.md hard
  rule). **Do not bump independently.** No newer verified pair adopted.
- `confluent-kafka 2.15.0`, `duckdb 1.4.5`, `dbt-duckdb 1.10.1`
  (dbt-core 1.12.0), `faker 37.12`, `pydantic 2.13.4`, `pyyaml 6.0.3` —
  all current stable lines.

Dev group (already fresh in `uv.lock`, resolved within existing constraints):

- `ruff 0.14.14`, `mypy 1.20.2`, `pytest 8.4.2`, `types-pyyaml 6.0.12` —
  current; no constraint changes needed, no bump commits required.

Scala module: Scala 2.13.16, ScalaTest 3.2.19, sbt 1.10.7
(`project/build.properties`) — current-enough; sbt 1.11.x exists but the
pinned 1.10.7 is what CI caches key on and works everywhere. Deferred.

### F3 (info): CI workflows already current

`ci.yml` + `demo.yml` use `actions/checkout@v4`, `astral-sh/setup-uv@v5`,
`actions/setup-java@v4` (temurin 21), `actions/cache@v4`,
`actions/upload-artifact@v4`, `sbt/setup-sbt@v1`,
`hashicorp/setup-terraform@v3` — all current majors. CI mirrors the local
gates run in this audit (lint/mypy, compat, governance drift, split
non-spark/spark pytest, hermetic dbt, scalafmt+scalatest+package, terraform
validate, compose config, weekly demo smoke). No workflow changes needed.

Note: CI's demo job was not exposed to F1 because `sbt/setup-sbt` installs
the official runner script, which accepts `-batch`; the fix keeps both paths
working.

### F4 (info): docs

README quickstart, CLAUDE.md commands, and `docs/architecture.md` match the
Makefile and module layout. No drift found; 7 ADRs indexed and referenced.

## Plan

1. `docs:` commit this audit plan. (done — this file)
2. `fix(gold):` remove `-batch` from `scripts/run_gold.sh`; verify with a
   full clean `make demo` (must end all-green including dbt_build and
   data_quality).
3. No dev-tool bumps needed (already latest within constraints, lock is
   fresh). No contract/governance changes, so no render diff to commit.
4. No CI changes needed (F3).
5. Update this file with Done vs Deferred and a summary.

## Done vs Deferred

(to be filled at the end of the pass)
