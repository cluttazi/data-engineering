"""Lightweight PySpark data-quality framework.

Why not Great Expectations: at this scale GE brings a large dependency tree,
version churn, and a Spark integration that lags Spark majors — while the
value we actually need is (a) declarative YAML suites reviewers can read,
(b) typed check implementations, (c) results as data (JSON + metrics rows)
that downstream consumers (report, dashboard, incident agent) can reason
about. That's ~400 lines of owned code. See
docs/adr/005-custom-dq-vs-great-expectations.md.

The marts layer gets its checks from dbt tests; this framework covers the
Spark-side tables (silver, gold) where dbt has no reach.
"""

# NOTE: the runner is intentionally not re-exported here — it is the
# `python -m quality.expectations.runner` entry point, and importing it at
# package level would trigger runpy's double-import warning.
from quality.expectations.suite import CheckResult, Suite, SuiteResult, load_suites

__all__ = ["CheckResult", "Suite", "SuiteResult", "load_suites"]
