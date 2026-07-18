"""Unit tests for the CDC simulator: envelope validity and determinism."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.cdc_simulator.envelope import ChangeEvent
from ingestion.cdc_simulator.simulator import (
    SimulationConfig,
    SimulationSummary,
    run_simulation,
)

REQUIRED_ENVELOPE_KEYS = {"before", "after", "op", "ts_ms", "source"}


class CollectingSink:
    """Test sink capturing events in memory."""

    def __init__(self) -> None:
        self.events: list[ChangeEvent] = []
        self.closed = False

    def emit(self, event: ChangeEvent) -> None:
        self.events.append(event)

    def close(self) -> None:
        self.closed = True


def run_collecting(events: int, seed: int = 42, corrupt_pct: float = 0.0) -> CollectingSink:
    sink = CollectingSink()
    config = SimulationConfig(events=events, seed=seed, corrupt_pct=corrupt_pct)
    run_simulation(config, sink=sink)
    return sink


def test_envelopes_are_valid_debezium_payloads() -> None:
    sink = run_collecting(events=300)
    assert len(sink.events) == 300
    assert sink.closed
    for event in sink.events:
        envelope = json.loads(event.line)
        assert envelope.keys() >= REQUIRED_ENVELOPE_KEYS
        assert envelope["op"] in {"c", "u", "d"}
        assert envelope["source"]["table"] == event.entity
        if envelope["op"] == "c":
            assert envelope["before"] is None and envelope["after"] is not None
        elif envelope["op"] == "u":
            assert envelope["before"] is not None and envelope["after"] is not None
        else:
            assert envelope["before"] is not None and envelope["after"] is None


def test_event_time_is_monotonic_per_run() -> None:
    sink = run_collecting(events=200)
    timestamps = [event.ts_ms for event in sink.events]
    assert timestamps == sorted(timestamps)


def test_same_seed_produces_identical_events() -> None:
    first = run_collecting(events=250, seed=7)
    second = run_collecting(events=250, seed=7)
    assert [e.line for e in first.events] == [e.line for e in second.events]


def test_different_seeds_diverge() -> None:
    first = run_collecting(events=100, seed=1)
    second = run_collecting(events=100, seed=2)
    assert [e.line for e in first.events] != [e.line for e in second.events]


def test_corrupt_pct_injects_unparseable_lines() -> None:
    sink = run_collecting(events=500, corrupt_pct=10.0)
    unparseable = 0
    for event in sink.events:
        try:
            envelope = json.loads(event.line)
            if REQUIRED_ENVELOPE_KEYS - envelope.keys():
                unparseable += 1
        except (json.JSONDecodeError, AttributeError):
            unparseable += 1
    assert unparseable > 0, "corruption was requested but every line parsed cleanly"


def test_corrupt_count_matches_summary() -> None:
    sink = CollectingSink()
    config = SimulationConfig(events=500, seed=42, corrupt_pct=10.0)
    summary: SimulationSummary = run_simulation(config, sink=sink)
    assert summary.corrupt_events > 0
    assert summary.events_emitted == 500


def test_updates_reference_previously_inserted_rows() -> None:
    sink = run_collecting(events=400)
    inserted_customers = set()
    for event in sink.events:
        envelope = json.loads(event.line)
        if envelope["source"]["table"] != "customers":
            continue
        if envelope["op"] == "c":
            inserted_customers.add(envelope["after"]["customer_id"])
        elif envelope["op"] == "u":
            assert envelope["after"]["customer_id"] in inserted_customers


def test_file_sink_layout_is_deterministic(tmp_path: Path) -> None:
    def run(target: Path) -> dict[str, str]:
        config = SimulationConfig(events=300, seed=42, landing_dir=target, rotate_every=100)
        run_simulation(config)
        return {
            str(path.relative_to(target)): path.read_text(encoding="utf-8")
            for path in sorted(target.rglob("*.ndjson"))
        }

    first = run(tmp_path / "a")
    second = run(tmp_path / "b")
    assert first == second
    assert any("transactions/" in name for name in first)


@pytest.mark.parametrize("events", [1, 10])
def test_tiny_runs_do_not_crash(events: int) -> None:
    sink = run_collecting(events=events)
    assert len(sink.events) == events
