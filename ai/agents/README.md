# Operational Agents

## Incident Reporter (`incident_reporter/`)

Reads the platform's own telemetry — the pipeline metrics export and the DQ
result JSONs — and produces a markdown incident report naming failing
steps, affected tables, probable causes, and runbook-aligned actions.

```bash
make agent                # report for the latest run -> data/reports/
uv run python -m ai.agents.incident_reporter --narrator claude   # optional LLM prose
```

### The architecture argument

The agent is split so that **the LLM is never load-bearing**:

1. **Detection is deterministic.** Typed `Detector` implementations
   (`FailedStepDetector`, `DqFailureDetector`, `QuarantineSpikeDetector`,
   `DurationAnomalyDetector`) emit `Finding` objects with severity,
   evidence, and recommendations. Statistical detectors use per-step
   historical baselines (mean + k·σ) and *say so in the evidence*, falling
   back to static thresholds when history is thin. All of it is unit-tested
   against synthetic bad-run fixtures.
2. **Narration is swappable.** The default template renderer is the
   guaranteed path. `ClaudeNarrativeGenerator` (gated on
   `ANTHROPIC_API_KEY`, selected only by explicit `--narrator claude`,
   stdlib-urllib so no SDK dependency) rewrites the summary prose from the
   structured findings at temperature 0 — and falls back to the template on
   any failure, because an incident report that fails during an incident is
   the worst bug this module could have.

This is the pattern that survives production review in regulated
environments: auditable rule-based decisions, with LLMs improving
readability at the edge rather than deciding what happened.

### Inputs are JVM-free by design

Metrics come from the Parquet export (pyarrow), DQ results from JSON — the
agent starts in milliseconds. An agent that needs a 40-second Spark session
to tell you the pipeline is down has failed at its one job.
