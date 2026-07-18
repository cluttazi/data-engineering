---
name: pipeline-scaffolder
description: Scaffold a new CDC entity end-to-end through the platform — contract, simulator support, silver config, dbt staging model, and DQ suite. Use when asked to add a new entity/table (e.g. "add a cards entity") to the lakehouse.
---

# Pipeline Scaffolder

Adding an entity touches six places, in dependency order. Work through them
all — a partially-wired entity fails in confusing ways (bronze accepts it,
silver has no contract, dbt sources 404).

## 1. Data contract (source of truth — do this first)

Create `quality/contracts/definitions/<entity>.v1.yaml`. Copy the shape of
`accounts.v1.yaml`. Requirements:
- `primary_key` fields must be `nullable: false`
- every `pii: true` field needs a `pii_category`
- pick `event_time_field` from the entity's own timestamps
- Verify: `uv run pytest tests/unit/quality/test_contracts.py`

## 2. Entity registration

Add the entity to the `entities:` list in `config/lakehouse.yaml` with its
`primary_key` and `scd` strategy (`type2` for mutable entities,
`append_only` for immutable facts). Bronze and silver iterate this list —
no pipeline code changes needed.

## 3. Simulator support

In `ingestion/cdc_simulator/domain.py`, add insert (and update, if type2)
methods on `BankingDomain` following the existing registry pattern:
seeded `self.rng` / `self.faker` only, timestamps via `self.clock.tick()`
+ `self._iso()` (never wall time). Register weights in
`ingestion/cdc_simulator/simulator.py::ACTION_WEIGHTS` and wire the maker
lambdas in `_make_event`. If the entity belongs in the operational schema,
mirror it in `platform/docker/initdb/01_banking_schema.sql`.

## 4. dbt

- Add the table under the `silver` source in
  `transform/dbt_project/models/sources.yml`.
- Create `models/staging/stg_<entity>.sql` — for SCD2 entities filter
  `is_current and not is_deleted`.
- Add key tests in `models/staging/schema.yml` (`unique`, `not_null` on the
  PK; `accepted_values` with `arguments:` for enums).
- Update `scripts/make_dbt_fixtures.py` with a minimal fixture table
  (include the SCD2 metadata columns) or the hermetic CI dbt job breaks.

## 5. DQ suite

Create `quality/expectations/suites/silver_<entity>.yaml` mirroring the
contract's rules (PK not_null/unique, enums, row_count, freshness with
`severity: warn`). Referential-integrity checks are `severity: warn` by
policy (see ADR 004).

## 6. Governance regeneration + verification

```bash
uv run python -m governance.unity_catalog.render   # PII tags from the new contract
uv run python -m quality.contracts.compat
make lint && make test
make clean && make demo                            # entity flows end-to-end
```

Commit as `feat(<entity>): add <entity> entity across contract, pipelines, dbt, and dq`.
