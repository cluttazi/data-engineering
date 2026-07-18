"""CLI entry point: ``python -m ingestion.cdc_simulator``."""

from __future__ import annotations

import argparse
import sys

from ingestion.cdc_simulator.simulator import SimulationConfig, run_simulation
from pipelines.common.config import load_config


def main(argv: list[str] | None = None) -> int:
    config = load_config()
    parser = argparse.ArgumentParser(
        prog="cdc_simulator",
        description="Emit Debezium-style CDC events for the synthetic banking domain.",
    )
    parser.add_argument("--events", type=int, default=2000, help="number of events to emit")
    parser.add_argument("--seed", type=int, default=42, help="deterministic seed")
    parser.add_argument(
        "--corrupt-pct",
        type=float,
        default=0.0,
        help="percent of events to corrupt (exercises bronze quarantine)",
    )
    parser.add_argument(
        "--sink",
        choices=["file", "kafka"],
        default="file",
        help="file: NDJSON landing zone (no infra); kafka: Redpanda from `make up`",
    )
    args = parser.parse_args(argv)

    sim_config = SimulationConfig(
        events=args.events,
        seed=args.seed,
        corrupt_pct=args.corrupt_pct,
        sink_kind=args.sink,
        landing_dir=config.source.cdc_landing_dir,
        bootstrap_servers=config.source.kafka.bootstrap_servers,
        topic_prefix=config.source.kafka.topic_prefix,
    )
    summary = run_simulation(sim_config)

    print(f"cdc_simulator: emitted {summary.events_emitted} events (sink={args.sink})")
    for (entity, op), count in sorted(summary.by_entity_op.items()):
        print(f"  {entity:18s} op={op}  {count}")
    if summary.corrupt_events:
        print(f"  corrupted (quarantine fodder): {summary.corrupt_events}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
