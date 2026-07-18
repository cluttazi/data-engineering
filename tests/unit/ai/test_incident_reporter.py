"""Incident reporter tests over synthetic bad-run fixtures."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ai.agents.incident_reporter.detectors import (
    DurationAnomalyDetector,
    FailedStepDetector,
    QuarantineSpikeDetector,
    run_detectors,
)
from ai.agents.incident_reporter.inputs import AgentInputs
from ai.agents.incident_reporter.narrative import TemplateNarrativeGenerator


def _metric(step: str, run_id: str = "run-now", **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "run_id": run_id,
        "pipeline": "bronze",
        "step": step,
        "layer": "bronze",
        "status": "success",
        "started_at": datetime(2026, 1, 1, tzinfo=UTC),
        "duration_s": 10.0,
        "rows_read": 1000,
        "rows_written": 990,
        "rows_quarantined": 10,
        "error": None,
    }
    row.update(overrides)
    return row


def _inputs(
    run_metrics: list[dict[str, Any]],
    history: list[dict[str, Any]] | None = None,
    dq_results: list[dict[str, Any]] | None = None,
) -> AgentInputs:
    return AgentInputs(
        run_id="run-now",
        run_metrics=run_metrics,
        history=history or [],
        dq_results=dq_results or [],
    )


def test_failed_step_detected_with_error_evidence() -> None:
    inputs = _inputs([_metric("cdc_stream", status="failed", error="AnalysisException: boom")])
    findings = FailedStepDetector().detect(inputs)
    assert len(findings) == 1
    assert findings[0].severity == "high"
    assert "AnalysisException: boom" in findings[0].evidence["error"]


def test_failed_gold_step_is_critical() -> None:
    inputs = _inputs([_metric("aggregates", layer="gold", status="failed", error="x")])
    assert FailedStepDetector().detect(inputs)[0].severity == "critical"


def test_quarantine_spike_static_threshold_when_history_thin() -> None:
    inputs = _inputs([_metric("cdc_stream", rows_read=100, rows_quarantined=20)])
    findings = QuarantineSpikeDetector().detect(inputs)
    assert len(findings) == 1
    assert "static threshold" in findings[0].evidence["basis"]


def test_quarantine_within_baseline_not_flagged() -> None:
    history = [
        _metric("cdc_stream", run_id=f"run-{i}", rows_read=1000, rows_quarantined=15)
        for i in range(4)
    ]
    inputs = _inputs([_metric("cdc_stream", rows_read=1000, rows_quarantined=16)], history=history)
    assert QuarantineSpikeDetector().detect(inputs) == []


def test_quarantine_spike_against_baseline() -> None:
    history = [
        _metric("cdc_stream", run_id=f"run-{i}", rows_read=1000, rows_quarantined=10)
        for i in range(4)
    ]
    inputs = _inputs([_metric("cdc_stream", rows_read=1000, rows_quarantined=200)], history=history)
    findings = QuarantineSpikeDetector().detect(inputs)
    assert len(findings) == 1
    assert "baseline" in findings[0].evidence["basis"]


def test_duration_anomaly_needs_history() -> None:
    slow = _inputs([_metric("silver_merge", duration_s=500.0)])
    assert DurationAnomalyDetector().detect(slow) == []

    history = [_metric("silver_merge", run_id=f"run-{i}", duration_s=10.0 + i) for i in range(5)]
    slow_with_history = _inputs([_metric("silver_merge", duration_s=500.0)], history=history)
    findings = DurationAnomalyDetector().detect(slow_with_history)
    assert len(findings) == 1
    assert findings[0].severity == "low"


def test_dq_error_produces_high_finding() -> None:
    dq = [
        {
            "table": "silver/customers",
            "layer": "silver",
            "row_count": 10,
            "checks": [
                {
                    "description": "unique(customer_id)",
                    "severity": "error",
                    "status": "fail",
                    "observed": "4 duplicate keys",
                },
            ],
        }
    ]
    findings = run_detectors(_inputs([_metric("s")], dq_results=dq))
    assert any(f.severity == "high" and "unique(customer_id)" in f.title for f in findings)


def test_findings_sorted_most_severe_first() -> None:
    dq = [
        {
            "table": "t",
            "layer": "silver",
            "row_count": 1,
            "checks": [
                {
                    "description": "freshness(x)",
                    "severity": "warn",
                    "status": "warn",
                    "observed": "old",
                }
            ],
        }
    ]
    inputs = _inputs(
        [_metric("a", status="failed", error="boom", layer="gold"), _metric("b")],
        dq_results=dq,
    )
    findings = run_detectors(inputs)
    severities = [f.severity for f in findings]
    assert severities == sorted(severities, key=["critical", "high", "medium", "low"].index)


def test_template_report_structure_healthy() -> None:
    report = TemplateNarrativeGenerator().render(_inputs([_metric("a")]), [])
    assert "Overall status: **HEALTHY**" in report
    assert "## Run timeline" in report
    assert "## Findings" not in report


def test_template_report_structure_with_findings() -> None:
    inputs = _inputs([_metric("a", status="failed", error="boom")])
    findings = run_detectors(inputs)
    report = TemplateNarrativeGenerator().render(inputs, findings)
    assert "SEV-2 CANDIDATE" in report
    assert "### 1. [HIGH] Pipeline step failed: bronze/a" in report
    assert "`boom" in report or "boom" in report
