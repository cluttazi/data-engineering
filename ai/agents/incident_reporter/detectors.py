"""Deterministic anomaly detectors over pipeline telemetry.

Each detector reads the assembled :class:`AgentInputs` and emits typed
:class:`Finding` objects with severity, evidence, and a recommendation drawn
from the runbook. Statistical detectors fall back to static thresholds when
history is too thin for a baseline — stated in the evidence, because an
honest agent says *how* it decided.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Literal, Protocol

from ai.agents.incident_reporter.inputs import AgentInputs

Severity = Literal["critical", "high", "medium", "low"]

SEVERITY_ORDER: dict[Severity, int] = {"critical": 0, "high": 1, "medium": 2, "low": 3}


@dataclass(frozen=True)
class Finding:
    severity: Severity
    title: str
    affected: str
    probable_cause: str
    recommendation: str
    evidence: dict[str, str] = field(default_factory=dict)


class Detector(Protocol):
    name: str

    def detect(self, inputs: AgentInputs) -> list[Finding]: ...


class FailedStepDetector:
    """Any step with status=failed is at least a high-severity incident."""

    name = "failed_step"

    def detect(self, inputs: AgentInputs) -> list[Finding]:
        findings = []
        for row in inputs.run_metrics:
            if row["status"] != "failed":
                continue
            severity: Severity = "critical" if row.get("layer") == "gold" else "high"
            findings.append(
                Finding(
                    severity=severity,
                    title=f"Pipeline step failed: {row['pipeline']}/{row['step']}",
                    affected=f"{row['pipeline']}/{row['step']}",
                    probable_cause=(
                        "Step raised an exception; see error evidence. Downstream layers "
                        "are stale from this point."
                    ),
                    recommendation=(
                        "Follow the SEV-2 path in the incident runbook: inspect the error, "
                        "check quarantine tables before touching checkpoints, re-run the "
                        "step after remediation."
                    ),
                    evidence={
                        "error": str(row.get("error") or "n/a"),
                        "duration_s": f"{row['duration_s']:.1f}",
                        "started_at": str(row["started_at"]),
                    },
                )
            )
        return findings


class DqFailureDetector:
    """Error-severity DQ failures are high; warns degrade to medium/low."""

    name = "dq_failure"

    def detect(self, inputs: AgentInputs) -> list[Finding]:
        findings = []
        for result in inputs.dq_results:
            failed = [c for c in result["checks"] if c["status"] == "fail"]
            warned = [c for c in result["checks"] if c["status"] == "warn"]
            for check in failed:
                findings.append(
                    Finding(
                        severity="high",
                        title=f"DQ check failed on {result['table']}: {check['description']}",
                        affected=result["table"],
                        probable_cause=(
                            "Contract-conformant data stopped arriving or a transformation "
                            "regressed; error-severity checks guard invariants."
                        ),
                        recommendation=(
                            "Block downstream promotion of this table; inspect recent "
                            "silver quarantine entries and the producing step's changes."
                        ),
                        evidence={"observed": check["observed"], "severity": check["severity"]},
                    )
                )
            for check in warned:
                is_ri = check["description"].startswith("referential_integrity")
                findings.append(
                    Finding(
                        severity="medium" if is_ri else "low",
                        title=f"DQ warning on {result['table']}: {check['description']}",
                        affected=result["table"],
                        probable_cause=(
                            "Orphaned references usually trace to quarantined upstream "
                            "events (corrupt CDC payloads), not defects in this table."
                            if is_ri
                            else "Non-blocking quality drift; monitor for trend."
                        ),
                        recommendation=(
                            "Cross-check the bronze quarantine table for the missing keys "
                            "and reprocess or backfill once payloads are fixed."
                            if is_ri
                            else "Review during the next business day per DQ standard."
                        ),
                        evidence={"observed": check["observed"]},
                    )
                )
        return findings


class QuarantineSpikeDetector:
    """Quarantine ratio vs. historical baseline (fallback: static threshold)."""

    name = "quarantine_spike"
    STATIC_THRESHOLD = 0.05
    MIN_HISTORY_RUNS = 3

    def detect(self, inputs: AgentInputs) -> list[Finding]:
        findings = []
        for row in inputs.run_metrics:
            read = row.get("rows_read") or 0
            quarantined = row.get("rows_quarantined") or 0
            if not read or not quarantined:
                continue
            ratio = quarantined / read

            historical = [
                (h.get("rows_quarantined") or 0) / h["rows_read"]
                for h in inputs.history
                if h["step"] == row["step"] and (h.get("rows_read") or 0) > 0
            ]
            if len(historical) >= self.MIN_HISTORY_RUNS:
                baseline = statistics.mean(historical)
                spread = statistics.pstdev(historical)
                threshold = baseline + max(2 * spread, 0.01)
                basis = f"baseline {baseline:.1%} +2sd over {len(historical)} runs"
            else:
                threshold = self.STATIC_THRESHOLD
                basis = f"static threshold (history too thin: {len(historical)} runs)"

            if ratio > threshold:
                findings.append(
                    Finding(
                        severity="medium",
                        title=f"Quarantine spike in {row['pipeline']}/{row['step']}",
                        affected=row["step"],
                        probable_cause=(
                            "Upstream payload change or producer bug increased the share "
                            "of malformed events."
                        ),
                        recommendation=(
                            "Sample the quarantine table's error_reason distribution; if a "
                            "contract change is upstream, follow contract evolution rather "
                            "than patching data."
                        ),
                        evidence={
                            "quarantine_ratio": f"{ratio:.1%}",
                            "threshold": f"{threshold:.1%}",
                            "basis": basis,
                            "rows_quarantined": str(quarantined),
                        },
                    )
                )
        return findings


class DurationAnomalyDetector:
    """Step duration beyond mean + 3 sigma of its own history."""

    name = "duration_anomaly"
    MIN_HISTORY_RUNS = 3

    def detect(self, inputs: AgentInputs) -> list[Finding]:
        findings = []
        for row in inputs.run_metrics:
            historical = [h["duration_s"] for h in inputs.history if h["step"] == row["step"]]
            if len(historical) < self.MIN_HISTORY_RUNS:
                continue
            mean = statistics.mean(historical)
            spread = statistics.pstdev(historical)
            threshold = mean + max(3 * spread, 0.5)
            if row["duration_s"] > threshold:
                findings.append(
                    Finding(
                        severity="low",
                        title=f"Duration anomaly in {row['pipeline']}/{row['step']}",
                        affected=row["step"],
                        probable_cause=(
                            "Data volume growth, skew, or resource contention; not "
                            "correctness-impacting yet."
                        ),
                        recommendation=(
                            "Compare rows_read against history; check for missing file "
                            "compaction or shuffle regression if volume is flat."
                        ),
                        evidence={
                            "duration_s": f"{row['duration_s']:.1f}",
                            "baseline_mean_s": f"{mean:.1f}",
                            "threshold_s": f"{threshold:.1f}",
                            "history_runs": str(len(historical)),
                        },
                    )
                )
        return findings


DEFAULT_DETECTORS: list[Detector] = [
    FailedStepDetector(),
    DqFailureDetector(),
    QuarantineSpikeDetector(),
    DurationAnomalyDetector(),
]


def run_detectors(inputs: AgentInputs, detectors: list[Detector] | None = None) -> list[Finding]:
    """All findings, most severe first."""
    findings: list[Finding] = []
    for detector in detectors or DEFAULT_DETECTORS:
        findings.extend(detector.detect(inputs))
    return sorted(findings, key=lambda f: (SEVERITY_ORDER[f.severity], f.title))
