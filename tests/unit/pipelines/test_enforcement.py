"""Contract-enforcement unit tests over hand-crafted events (spark-marked)."""

from __future__ import annotations

import json
from collections.abc import Mapping

import pytest
from pyspark.sql import DataFrame, SparkSession

from pipelines.silver.enforcement import enforce_contract, parse_payloads
from quality.contracts.loader import load_contracts

pytestmark = pytest.mark.spark


Event = tuple[str, str, int, int, str]


def _event(op: str, after: Mapping[str, object] | None, ts_ms: int = 1_767_225_600_000) -> Event:
    raw = json.dumps({"before": None, "after": after, "op": op, "ts_ms": ts_ms})
    return (raw, op, ts_ms, ts_ms, "run-test")


def _events_df(spark: SparkSession, rows: list[Event]) -> DataFrame:
    return spark.createDataFrame(
        rows, schema="raw_value string, op string, ts_ms long, lsn long, run_id string"
    )


VALID_CUSTOMER = {
    "customer_id": "CUST-1",
    "full_name": "Taro Test",
    "email": "taro@example.com",
    "phone": None,
    "address": None,
    "segment": "retail",
    "risk_rating": "low",
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}


def test_valid_rows_pass_with_types(spark: SparkSession) -> None:
    contract = load_contracts()["customers"]
    df = parse_payloads(_events_df(spark, [_event("c", VALID_CUSTOMER)]), contract)
    result = enforce_contract(df, contract)
    assert result.quarantined.count() == 0
    row = result.valid.select("after.*").first()
    assert row is not None
    assert row["segment"] == "retail"
    assert str(row["created_at"].tzinfo) is not None  # typed timestamp, not string


def test_null_required_field_quarantined_with_reason(spark: SparkSession) -> None:
    contract = load_contracts()["customers"]
    broken = {**VALID_CUSTOMER, "email": None}
    df = parse_payloads(_events_df(spark, [_event("c", broken)]), contract)
    result = enforce_contract(df, contract)
    assert result.valid.count() == 0
    quarantined = result.quarantined.first()
    assert quarantined is not None
    assert "null_violation:email" in quarantined["violations"]


def test_enum_violation_quarantined(spark: SparkSession) -> None:
    contract = load_contracts()["customers"]
    broken = {**VALID_CUSTOMER, "segment": "vip"}
    df = parse_payloads(_events_df(spark, [_event("c", broken)]), contract)
    result = enforce_contract(df, contract)
    quarantined = result.quarantined.first()
    assert quarantined is not None
    assert "enum_violation:segment" in quarantined["violations"]


def test_multiple_violations_all_reported(spark: SparkSession) -> None:
    contract = load_contracts()["customers"]
    broken = {**VALID_CUSTOMER, "segment": "vip", "full_name": None}
    df = parse_payloads(_events_df(spark, [_event("c", broken)]), contract)
    result = enforce_contract(df, contract)
    quarantined = result.quarantined.first()
    assert quarantined is not None
    assert set(quarantined["violations"]) >= {"enum_violation:segment", "null_violation:full_name"}


def test_delete_events_bypass_after_checks(spark: SparkSession) -> None:
    contract = load_contracts()["customers"]
    df = parse_payloads(_events_df(spark, [_event("d", None)]), contract)
    result = enforce_contract(df, contract)
    assert result.quarantined.count() == 0
    assert result.valid.count() == 1


def test_untyped_garbage_payload_quarantined(spark: SparkSession) -> None:
    """A structurally valid envelope whose after-image is not an object."""
    contract = load_contracts()["customers"]
    raw = json.dumps({"before": None, "after": "not-an-object", "op": "c", "ts_ms": 1})
    df = parse_payloads(_events_df(spark, [(raw, "c", 1, 1, "run-test")]), contract)
    result = enforce_contract(df, contract)
    quarantined = result.quarantined.first()
    assert quarantined is not None
    # PERMISSIVE parsing turns a non-object payload into an all-null struct,
    # so the row is caught by the per-field nullability rules.
    assert any(v.startswith("null_violation:") for v in quarantined["violations"])
    assert result.valid.count() == 0
