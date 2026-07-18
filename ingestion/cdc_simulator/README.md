# CDC Simulator

Emits Debezium-style change events for the synthetic retail-banking domain
(customers, accounts, transactions, loan applications), playing the role of a
Debezium Postgres connector attached to the schema in
`platform/docker/initdb/`.

```bash
uv run python -m ingestion.cdc_simulator --events 2000 --seed 42 --corrupt-pct 2
# or against Redpanda after `make up`:
uv run python -m ingestion.cdc_simulator --sink kafka
```

## Why it exists

The demo must run with **zero infrastructure**, but the pipelines must be
production-shaped. Solving both at once means the *transport* is swappable
while the *payload* is fixed: this simulator emits the exact
`before/after/op/ts_ms/source` envelope a real connector produces, either to
NDJSON files (default) or to Kafka. Bronze cannot tell the difference — so
swapping in a real Debezium deployment later touches nothing downstream.

## Design decisions

- **Stateful domain, faithful `before` images.** Updates and deletes carry the
  true previous row, like logical replication does. This is what makes the
  silver SCD2 merge honest — you can't demonstrate CDC handling with
  insert-only data.
- **Deterministic by construction.** One seeded `random.Random`, a seeded
  `Faker`, and a seeded *logical clock* (wall time never enters the payload).
  Same seed → byte-identical output. Tests and the demo's row-count
  assertions depend on this.
- **Deliberate corruption** (`--corrupt-pct`): a fraction of lines are
  truncated / type-mangled before the sink. Real CDC feeds carry garbage;
  the bronze quarantine path needs something real to catch, and the
  integration tests assert the exact count.
- **Workload shape**: transactions dominate (~45%), reference entities churn
  slowly, loan applications follow a status workflow
  (`submitted → under_review → approved/rejected`) and are occasionally
  deleted after rejection — giving downstream SCD2 genuine update chains and
  a GDPR-style erasure case.

## Event envelope

```json
{
  "before": null,
  "after": {"customer_id": "CUST-0000001", "...": "..."},
  "op": "c",
  "ts_ms": 1767225601234,
  "source": {"connector": "postgresql", "table": "customers", "lsn": 17, "...": "..."}
}
```
