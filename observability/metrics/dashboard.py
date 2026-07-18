"""Streamlit dashboard over the metrics Parquet export.

Reads Parquet with duckdb rather than the Delta table with Spark — the
dashboard should start in seconds with no JVM. Run ``make report`` first
(or ``make demo``, which does) to refresh the export, then::

    uv sync --extra dashboard
    make dashboard
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import streamlit as st

from pipelines.common.config import load_config

st.set_page_config(page_title="Lakehouse Observability", layout="wide")

config = load_config()
metrics_glob = str(
    config.storage.exports_dir / "observability" / "pipeline_run_metrics" / "*.parquet"
)

if not list(Path(metrics_glob).parent.glob("*.parquet")):
    st.warning("No metrics export found. Run `make demo` (or `make report`) first.")
    st.stop()

con = duckdb.connect()
metrics = con.execute(
    f"select * from read_parquet('{metrics_glob}') order by started_at"
).fetch_df()

st.title("Pipeline Observability")

runs = metrics["run_id"].unique().tolist()
selected_run = st.sidebar.selectbox("Run", options=list(reversed(runs)))
run_df = metrics[metrics["run_id"] == selected_run]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Steps", len(run_df))
col2.metric("Failed", int((run_df["status"] == "failed").sum()))
col3.metric("Rows written", int(run_df["rows_written"].fillna(0).sum()))
col4.metric("Quarantined", int(run_df["rows_quarantined"].fillna(0).sum()))

st.subheader("Step timeline")
st.dataframe(
    run_df[
        [
            "pipeline",
            "step",
            "layer",
            "status",
            "duration_s",
            "rows_read",
            "rows_written",
            "rows_quarantined",
            "dq_pass_rate",
            "error",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

st.subheader("Step durations (s)")
st.bar_chart(run_df.set_index("step")["duration_s"])

st.subheader("DQ pass rate by suite")
dq = run_df[run_df["dq_pass_rate"].notna()]
if len(dq):
    st.bar_chart(dq.set_index("step")["dq_pass_rate"])
else:
    st.caption("No DQ metrics in this run.")

st.subheader("Runs over time")
history = con.execute(
    f"""
    select run_id,
           min(started_at) as started,
           sum(coalesce(rows_written, 0)) as rows_written,
           sum(case when status = 'failed' then 1 else 0 end) as failed_steps
    from read_parquet('{metrics_glob}')
    group by run_id
    order by started
    """
).fetch_df()
st.dataframe(history, use_container_width=True, hide_index=True)
