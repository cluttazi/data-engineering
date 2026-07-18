"""Bronze layer: raw, immutable ingestion with quarantine.

Bronze stores the Debezium envelope *as received* (raw JSON line + parsed
envelope metadata) — no business typing, no contract knowledge. Unparseable
or structurally broken events land in ``bronze/quarantine`` with a reason,
never blocking the healthy majority. See docs/adr/004-quarantine-vs-fail-fast.md.
"""
