"""COPY INTO-semantics regression tests: exactly-once per file path.

The scheme-normalization bug this guards against: Spark's
``_metadata.file_path`` yields ``file://`` URIs while the landing zone is
listed with plain paths — if the ledger stores URIs, the set difference
never matches and every run silently reloads everything.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from ingestion.batch_landing.generator import generate_all
from pipelines.bronze.copy_into import batch_table_path, ledger_path, run_copy_into
from tests.conftest import make_config

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_rerun_is_noop_and_resend_is_loaded(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> None:
    config = make_config(tmp_path_factory.mktemp("copy-into"))
    generate_all(config.source.batch_landing_dir, seed=42)

    first = run_copy_into(config, spark)
    assert first["branches"] == 50  # 25 rows x 2 files (primary + resend)
    assert first["products"] == 9
    assert first["fx_rates"] == 5

    # exactly-once: same landing zone, zero new loads
    second = run_copy_into(config, spark)
    assert second == {"branches": 0, "products": 0, "fx_rates": 0}

    branches = spark.read.format("delta").load(batch_table_path(config, "branches"))
    assert branches.count() == 50

    # ledger stores plain paths that match the landing zone listing
    ledger = spark.read.format("delta").load(ledger_path(config))
    paths = [r["file_path"] for r in ledger.select("file_path").collect()]
    assert paths and all(not p.startswith("file:") for p in paths)
