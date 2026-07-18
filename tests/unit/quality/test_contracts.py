"""Contract model + loader tests (no Spark required except schema compile)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DecimalType

from quality.contracts.loader import DEFINITIONS_DIR, load_contracts
from quality.contracts.models import Contract
from quality.contracts.spark_schema import to_spark_type, to_struct_type

BASE: dict[str, Any] = {
    "contract": "things",
    "version": 1,
    "owner": "team@bank.example",
    "description": "test",
    "primary_key": ["thing_id"],
    "event_time_field": "updated_at",
    "sla": {"freshness_hours": 24},
    "fields": [
        {"name": "thing_id", "type": "string", "nullable": False},
        {"name": "updated_at", "type": "timestamp", "nullable": False},
    ],
}


def test_all_repo_contracts_load_and_compile() -> None:
    contracts = load_contracts(DEFINITIONS_DIR)
    assert set(contracts) == {"customers", "accounts", "transactions", "loan_applications"}
    for contract in contracts.values():
        struct = to_struct_type(contract)
        assert len(struct.fields) == len(contract.fields)
        assert contract.pii_fields, f"{contract.contract} declares no PII — banking data does"


def test_primary_key_must_exist_and_be_non_nullable() -> None:
    bad = {**BASE, "primary_key": ["missing_field"]}
    with pytest.raises(ValidationError, match="primary_key fields not defined"):
        Contract.model_validate(bad)

    nullable_pk = {
        **BASE,
        "fields": [
            {"name": "thing_id", "type": "string", "nullable": True},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
        ],
    }
    with pytest.raises(ValidationError, match="must be nullable: false"):
        Contract.model_validate(nullable_pk)


def test_pii_requires_category_and_vice_versa() -> None:
    missing_category = {
        **BASE,
        "fields": [
            *BASE["fields"],
            {"name": "email", "type": "string", "pii": True},
        ],
    }
    with pytest.raises(ValidationError, match="lacks pii_category"):
        Contract.model_validate(missing_category)


def test_unknown_type_rejected() -> None:
    bad_type = {
        **BASE,
        "fields": [
            {"name": "thing_id", "type": "varchar(99)", "nullable": False},
            {"name": "updated_at", "type": "timestamp", "nullable": False},
        ],
    }
    with pytest.raises(ValidationError, match="unsupported contract type"):
        Contract.model_validate(bad_type)


def test_decimal_type_compiles() -> None:
    assert to_spark_type("decimal(18,2)") == DecimalType(18, 2)
    assert to_spark_type("decimal(10, 4)") == DecimalType(10, 4)
