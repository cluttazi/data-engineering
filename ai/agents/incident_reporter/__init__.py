"""Incident reporter: metrics + DQ results in, actionable report out.

Two-stage architecture, and the split is the point:

1. **Detection is deterministic** — typed detectors over the metrics export
   and DQ JSON produce evidence-backed findings. Testable, auditable, and
   correct with no model in the loop.
2. **Narration is swappable** — a template generator renders the report by
   default; a Claude-backed generator (gated on ``ANTHROPIC_API_KEY``,
   never required) can improve the prose. The LLM is allowed to make the
   report *nicer*, never to decide *what happened*.
"""
