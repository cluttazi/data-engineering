"""CLI: ``python -m ai.agents.incident_reporter [--run-id ...] [--narrator template|claude]``."""

from __future__ import annotations

import argparse
import sys

from ai.agents.incident_reporter.detectors import run_detectors
from ai.agents.incident_reporter.inputs import load_inputs
from ai.agents.incident_reporter.narrative import (
    ClaudeNarrativeGenerator,
    NarrativeGenerator,
    TemplateNarrativeGenerator,
)
from pipelines.common.config import load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="incident_reporter",
        description="Generate an incident report from pipeline metrics and DQ results.",
    )
    parser.add_argument("--run-id", type=str, default=None, help="default: most recent run")
    parser.add_argument(
        "--narrator",
        choices=["template", "claude"],
        default="template",
        help="claude requires ANTHROPIC_API_KEY and is never used implicitly",
    )
    args = parser.parse_args(argv)

    config = load_config()
    inputs = load_inputs(config, run_id=args.run_id)
    findings = run_detectors(inputs)

    narrator: NarrativeGenerator = (
        ClaudeNarrativeGenerator() if args.narrator == "claude" else TemplateNarrativeGenerator()
    )
    report = narrator.render(inputs, findings)

    config.storage.reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = config.storage.reports_dir / f"incident_{inputs.run_id}.md"
    out_path.write_text(report, encoding="utf-8")

    print(f"incident report -> {out_path}")
    print(f"  findings: {len(findings)}")
    for finding in findings:
        print(f"  [{finding.severity:8s}] {finding.title}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
