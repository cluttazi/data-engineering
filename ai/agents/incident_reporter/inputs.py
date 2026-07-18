"""Input loading for the incident reporter — deliberately JVM-free.

Metrics come from the Parquet export written by ``make report`` (pyarrow
read, no Spark session); DQ results come from the runner's JSON files.
An agent that needs a 40-second Spark startup to tell you the pipeline is
broken has failed at its one job.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pyarrow.parquet as pq

from pipelines.common.config import LakehouseConfig

MetricRow = dict[str, Any]
DqResult = dict[str, Any]


@dataclass(frozen=True)
class AgentInputs:
    run_id: str
    run_metrics: list[MetricRow]  # steps of the run under analysis
    history: list[MetricRow]  # all other runs, for baselines
    dq_results: list[DqResult]


def load_metrics(config: LakehouseConfig) -> list[MetricRow]:
    export = config.storage.exports_dir / "observability" / "pipeline_run_metrics"
    files = sorted(export.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"no metrics export under {export} — run `make report` (or `make demo`) first"
        )
    rows: list[MetricRow] = []
    for file in files:
        table = pq.read_table(file)  # type: ignore[no-untyped-call]  # pyarrow io is unannotated
        rows.extend(table.to_pylist())
    return rows


def load_dq_results(config: LakehouseConfig) -> list[DqResult]:
    dq_dir = config.storage.run_dir / "dq"
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(dq_dir.glob("*.json"))]


def load_inputs(config: LakehouseConfig, run_id: str | None = None) -> AgentInputs:
    """Assemble inputs for one run (default: the most recent by start time)."""
    metrics = load_metrics(config)
    if not metrics:
        raise RuntimeError("metrics export is empty")
    if run_id is None:
        run_id = max(metrics, key=lambda r: r["started_at"])["run_id"]
    return AgentInputs(
        run_id=run_id,
        run_metrics=sorted(
            (r for r in metrics if r["run_id"] == run_id), key=lambda r: r["started_at"]
        ),
        history=[r for r in metrics if r["run_id"] != run_id],
        dq_results=load_dq_results(config),
    )
