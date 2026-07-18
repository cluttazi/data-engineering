"""Event-loop that turns domain mutations into Debezium-style change events.

The mix of operations is weighted to look like a real retail-banking workload
(transactions dominate; reference entities churn slowly). A configurable
fraction of events is deliberately corrupted before serialization — those lines
must survive transport untouched and end up in the bronze quarantine table,
which the integration tests assert by exact count.
"""

from __future__ import annotations

import random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from ingestion.cdc_simulator.domain import BankingDomain
from ingestion.cdc_simulator.envelope import ChangeEvent, Op, build_envelope, serialize
from ingestion.cdc_simulator.sinks import FileSink, KafkaSink, Sink

# (entity, action) -> weight; actions map to BankingDomain methods below.
ACTION_WEIGHTS: list[tuple[str, str, int]] = [
    ("customers", "insert", 8),
    ("customers", "update", 5),
    ("accounts", "insert", 10),
    ("accounts", "update", 12),
    ("transactions", "insert", 45),
    ("loan_applications", "insert", 8),
    ("loan_applications", "update", 10),
    ("loan_applications", "delete", 2),
]

# Seed the world before the weighted mix so updates/transactions have targets.
BOOTSTRAP_CUSTOMERS = 25
BOOTSTRAP_ACCOUNTS = 40


@dataclass
class SimulationConfig:
    events: int
    seed: int = 42
    corrupt_pct: float = 0.0
    sink_kind: str = "file"  # file | kafka
    landing_dir: Path = Path("data/landing/cdc")
    rotate_every: int = 500
    bootstrap_servers: str = "localhost:19092"
    topic_prefix: str = "banking.cdc"


@dataclass
class SimulationSummary:
    events_emitted: int = 0
    corrupt_events: int = 0
    by_entity_op: Counter[tuple[str, str]] = field(default_factory=Counter)

    def record(self, entity: str, op: str) -> None:
        self.events_emitted += 1
        self.by_entity_op[(entity, op)] += 1


def _corrupt_line(line: str, rng: random.Random) -> str:
    """Damage a serialized envelope in one of three realistic ways."""
    variant = rng.choice(["truncate", "bad_ts", "not_json"])
    if variant == "truncate":
        return line[: max(10, len(line) // 2)]
    if variant == "bad_ts":
        return line.replace('"ts_ms":', '"ts_ms":"corrupted-', 1)
    return "GARBAGE " + line[:40]


Row = dict[str, object]
MadeEvent = tuple[str, Op, Row | None, Row | None]


def _insert_event(row: Row | None, key_field: str) -> MadeEvent | None:
    return None if row is None else (str(row[key_field]), "c", None, row)


def _update_event(result: tuple[Row, Row] | None, key_field: str) -> MadeEvent | None:
    if result is None:
        return None
    before, after = result
    return str(after[key_field]), "u", before, after


def _delete_rejected_loan(domain: BankingDomain) -> MadeEvent | None:
    """Purge one rejected application (GDPR-style erasure event)."""
    rejected = sorted(
        app_id for app_id, row in domain.loan_applications.items() if row["status"] == "rejected"
    )
    if not rejected:
        return None
    app_id = domain.rng.choice(rejected)
    return str(app_id), "d", domain.loan_applications.pop(app_id), None


def _make_event(domain: BankingDomain, entity: str, action: str) -> MadeEvent | None:
    """Run one domain mutation; returns (key, op, before, after) or None."""
    makers: dict[tuple[str, str], Callable[[], MadeEvent | None]] = {
        ("customers", "insert"): lambda: _insert_event(domain.insert_customer(), "customer_id"),
        ("customers", "update"): lambda: _update_event(domain.update_customer(), "customer_id"),
        ("accounts", "insert"): lambda: _insert_event(domain.insert_account(), "account_id"),
        ("accounts", "update"): lambda: _update_event(domain.update_account(), "account_id"),
        ("transactions", "insert"): lambda: _insert_event(
            domain.insert_transaction(), "transaction_id"
        ),
        ("loan_applications", "insert"): lambda: _insert_event(
            domain.insert_loan_application(), "application_id"
        ),
        ("loan_applications", "update"): lambda: _update_event(
            domain.advance_loan_application(), "application_id"
        ),
        ("loan_applications", "delete"): lambda: _delete_rejected_loan(domain),
    }
    return makers[(entity, action)]()


def build_sink(config: SimulationConfig) -> Sink:
    if config.sink_kind == "kafka":
        return KafkaSink(config.bootstrap_servers, config.topic_prefix)
    return FileSink(config.landing_dir, config.rotate_every)


def run_simulation(config: SimulationConfig, sink: Sink | None = None) -> SimulationSummary:
    """Emit ``config.events`` change events to the sink; returns emission stats."""
    domain = BankingDomain(config.seed)
    sink = sink if sink is not None else build_sink(config)
    summary = SimulationSummary()
    lsn = 0

    entities = [(e, a) for e, a, w in ACTION_WEIGHTS]
    weights = [w for _, _, w in ACTION_WEIGHTS]

    plan: list[tuple[str, str]] = []
    bootstrap = min(config.events, BOOTSTRAP_CUSTOMERS + BOOTSTRAP_ACCOUNTS)
    plan += [("customers", "insert")] * min(bootstrap, BOOTSTRAP_CUSTOMERS)
    plan += [("accounts", "insert")] * max(0, bootstrap - BOOTSTRAP_CUSTOMERS)
    while len(plan) < config.events:
        plan.append(domain.rng.choices(entities, weights=weights, k=1)[0])

    try:
        for planned_entity, action in plan:
            entity = planned_entity
            made = _make_event(domain, entity, action)
            if made is None:  # preconditions unmet (e.g. nothing to update yet)
                entity = "customers"
                made = _make_event(domain, entity, "insert")
                assert made is not None
            key, op, before, after = made
            lsn += 1
            ts_ms = domain.clock.now_ms
            line = serialize(
                build_envelope(
                    entity=entity, op=op, ts_ms=ts_ms, lsn=lsn, before=before, after=after
                )
            )
            if config.corrupt_pct > 0 and domain.rng.random() < config.corrupt_pct / 100:
                line = _corrupt_line(line, domain.rng)
                summary.corrupt_events += 1
            sink.emit(ChangeEvent(entity=entity, key=key, line=line, ts_ms=ts_ms))
            summary.record(entity, op)
    finally:
        sink.close()
    return summary
