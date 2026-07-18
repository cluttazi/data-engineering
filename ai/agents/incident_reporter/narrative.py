"""Report narration: template by default, Claude-backed when explicitly enabled.

The deterministic detectors decide *what happened*; a narrative generator
only decides *how it reads*. ``TemplateNarrativeGenerator`` is the always-on
path. ``ClaudeNarrativeGenerator`` is optional and gated on
``ANTHROPIC_API_KEY`` — it rewrites the summary prose from the structured
findings (never inventing new facts, temperature 0) and appends the same
findings sections verbatim, so every claim in the report stays traceable to
detector evidence. Implemented over stdlib urllib so the repo takes no SDK
dependency for an optional feature.
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import UTC, datetime
from typing import Protocol

from ai.agents.incident_reporter.detectors import Finding
from ai.agents.incident_reporter.inputs import AgentInputs

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-5"


class NarrativeGenerator(Protocol):
    def render(self, inputs: AgentInputs, findings: list[Finding]) -> str: ...


def _overall_status(findings: list[Finding]) -> str:
    severities = {f.severity for f in findings}
    if "critical" in severities:
        return "SEV-1 CANDIDATE"
    if "high" in severities:
        return "SEV-2 CANDIDATE"
    if "medium" in severities:
        return "SEV-3 / DEGRADED"
    if severities:
        return "HEALTHY WITH NOTES"
    return "HEALTHY"


def _findings_sections(findings: list[Finding]) -> list[str]:
    sections = []
    for i, finding in enumerate(findings, start=1):
        evidence = "\n".join(f"- {key}: `{value}`" for key, value in finding.evidence.items())
        sections.append(
            f"### {i}. [{finding.severity.upper()}] {finding.title}\n\n"
            f"**Affected:** `{finding.affected}`\n\n"
            f"**Probable cause:** {finding.probable_cause}\n\n"
            f"**Evidence:**\n{evidence}\n\n"
            f"**Recommended action:** {finding.recommendation}\n"
        )
    return sections


def _run_table(inputs: AgentInputs) -> str:
    lines = [
        "| step | status | secs | read | written | quarantined |",
        "|---|---|---|---|---|---|",
    ]
    for row in inputs.run_metrics:
        lines.append(
            f"| {row['pipeline']}/{row['step']} | {row['status']} | "
            f"{row['duration_s']:.1f} | {row.get('rows_read') or '—'} | "
            f"{row.get('rows_written') or '—'} | {row.get('rows_quarantined') or '—'} |"
        )
    return "\n".join(lines)


class TemplateNarrativeGenerator:
    """Deterministic markdown report; the guaranteed, testable path."""

    def render(self, inputs: AgentInputs, findings: list[Finding]) -> str:
        status = _overall_status(findings)
        by_severity: dict[str, int] = {}
        for finding in findings:
            by_severity[finding.severity] = by_severity.get(finding.severity, 0) + 1
        counts = (
            ", ".join(f"{count} {sev}" for sev, count in sorted(by_severity.items()))
            if findings
            else "no findings"
        )

        parts = [
            f"# Pipeline Incident Report — run `{inputs.run_id}`",
            "",
            f"Generated: {datetime.now(tz=UTC).isoformat(timespec='seconds')}  ",
            f"Overall status: **{status}** ({counts})",
            "",
            "## Summary",
            "",
        ]
        if findings:
            worst = findings[0]
            parts.append(
                f"The run completed with {len(findings)} finding(s). Most severe: "
                f"[{worst.severity.upper()}] {worst.title} — {worst.probable_cause}"
            )
        else:
            parts.append(
                "All steps succeeded and all data-quality suites passed. "
                "No anomalies against historical baselines."
            )
        parts += ["", "## Run timeline", "", _run_table(inputs), ""]
        if findings:
            parts += ["## Findings", "", *_findings_sections(findings)]
        parts += [
            "---",
            "*Produced by the deterministic incident-reporter agent "
            "(ai/agents/incident_reporter); detection is rule-based over the "
            "metrics table and DQ results.*",
        ]
        return "\n".join(parts)


class ClaudeNarrativeGenerator:
    """Optional LLM narration: better prose, same facts.

    Requires ``ANTHROPIC_API_KEY``; never used unless explicitly selected
    (``--narrator claude``). Falls back to the template on any failure —
    an incident report that fails to generate during an incident is the
    worst possible bug in this file.
    """

    def __init__(self, model: str | None = None) -> None:
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.model = model or os.environ.get("ANTHROPIC_MODEL", DEFAULT_MODEL)
        self._fallback = TemplateNarrativeGenerator()

    def render(self, inputs: AgentInputs, findings: list[Finding]) -> str:
        if not self.api_key:
            return self._fallback.render(inputs, findings)
        try:
            summary = self._generate_summary(inputs, findings)
        except Exception:
            return self._fallback.render(inputs, findings)

        base = self._fallback.render(inputs, findings)
        # Replace only the summary prose; findings/evidence stay verbatim.
        head, _, tail = base.partition("## Summary")
        _, _, rest = tail.partition("## ")
        return f"{head}## Summary\n\n{summary}\n\n## {rest}"

    def _generate_summary(self, inputs: AgentInputs, findings: list[Finding]) -> str:
        payload = {
            "model": self.model,
            "max_tokens": 500,
            "temperature": 0,
            "system": (
                "You are an SRE writing the summary paragraph of a data-pipeline "
                "incident report. Use ONLY the structured findings provided; do not "
                "invent facts, numbers, or causes. Two short paragraphs maximum."
            ),
            "messages": [
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "run_id": inputs.run_id,
                            "findings": [
                                {
                                    "severity": f.severity,
                                    "title": f.title,
                                    "probable_cause": f.probable_cause,
                                    "evidence": f.evidence,
                                }
                                for f in findings
                            ],
                        }
                    ),
                }
            ],
        }
        request = urllib.request.Request(
            ANTHROPIC_API_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read())
        text: str = body["content"][0]["text"]
        return text.strip()
