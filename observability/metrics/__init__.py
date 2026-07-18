"""Pipeline run metrics: one Delta table, one writer, consumed everywhere.

Every pipeline step (Python and Scala alike) appends rows to
``<lakehouse_root>/observability/pipeline_run_metrics`` through the same
schema. The static report, the Streamlit dashboard, and the incident-report
agent are all just readers of this table — the classic "metrics as data"
pattern, kept deliberately boring.
"""
