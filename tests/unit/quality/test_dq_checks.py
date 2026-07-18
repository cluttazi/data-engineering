"""DQ check implementations over hand-built frames (spark-marked)."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession

from quality.expectations.checks import REFERENCE_TIME_ENV, run_check
from quality.expectations.suite import (
    AcceptedValuesCheck,
    FreshnessCheck,
    NotNullCheck,
    ReferentialIntegrityCheck,
    RowCountBetweenCheck,
    UniqueCheck,
    load_suites,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("A", "retail", "2026-01-01 10:00:00"),
            ("B", "retail", "2026-01-01 11:00:00"),
            ("B", "vip", None),
        ],
        schema="id string, segment string, updated_at string",
    ).selectExpr("id", "segment", "cast(updated_at as timestamp) as updated_at")


def test_not_null(spark: SparkSession, df: DataFrame) -> None:
    ok = run_check(spark, df, NotNullCheck(type="not_null", column="id"), {})
    bad = run_check(spark, df, NotNullCheck(type="not_null", column="updated_at"), {})
    assert ok.passed
    assert not bad.passed
    assert "1 null" in bad.observed


def test_unique(spark: SparkSession, df: DataFrame) -> None:
    result = run_check(spark, df, UniqueCheck(type="unique", columns=["id"]), {})
    assert not result.passed
    assert "1 duplicate" in result.observed


def test_accepted_values(spark: SparkSession, df: DataFrame) -> None:
    result = run_check(
        spark,
        df,
        AcceptedValuesCheck(type="accepted_values", column="segment", values=["retail"]),
        {},
    )
    assert not result.passed
    assert "1 out-of-domain" in result.observed


def test_row_count_bounds(spark: SparkSession, df: DataFrame) -> None:
    ok = run_check(
        spark, df, RowCountBetweenCheck(type="row_count_between", min_rows=1, max_rows=10), {}
    )
    too_few = run_check(spark, df, RowCountBetweenCheck(type="row_count_between", min_rows=100), {})
    assert ok.passed
    assert not too_few.passed


def test_freshness_pinned_reference(
    spark: SparkSession, df: DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    check = FreshnessCheck(type="freshness", column="updated_at", max_age_hours=24)
    monkeypatch.setenv(REFERENCE_TIME_ENV, "2026-01-01T20:00:00+00:00")
    fresh = run_check(spark, df, check, {})
    monkeypatch.setenv(REFERENCE_TIME_ENV, "2026-01-10T00:00:00+00:00")
    stale = run_check(spark, df, check, {})
    assert fresh.passed
    assert not stale.passed


def test_referential_integrity(spark: SparkSession, df: DataFrame) -> None:
    ref = spark.createDataFrame([("A",)], schema="ref_id string")
    check = ReferentialIntegrityCheck(
        type="referential_integrity",
        column="id",
        ref_table="whatever",
        ref_column="ref_id",
        severity="warn",
    )
    result = run_check(spark, df, check, {"whatever": ref})
    assert not result.passed
    assert "2 orphaned" in result.observed  # both B rows
    assert result.status == "warn"


def test_all_repo_suites_load(spark: SparkSession) -> None:
    suites = load_suites()
    assert {s.table for s in suites} >= {
        "silver/customers",
        "silver/accounts",
        "silver/transactions",
        "gold/account_daily_metrics",
    }
