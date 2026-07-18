"""Debezium-style CDC event simulator for the synthetic retail-banking domain.

The simulator plays the role of a Debezium Postgres connector attached to the
``banking`` schema (see ``platform/docker/initdb``): it emits change events with
the standard ``before/after/op/ts_ms/source`` envelope, either to NDJSON files
(default, zero infrastructure) or to a Kafka-compatible broker.
"""

from ingestion.cdc_simulator.domain import BankingDomain
from ingestion.cdc_simulator.envelope import ChangeEvent, build_envelope
from ingestion.cdc_simulator.simulator import SimulationConfig, run_simulation

__all__ = [
    "BankingDomain",
    "ChangeEvent",
    "SimulationConfig",
    "build_envelope",
    "run_simulation",
]
