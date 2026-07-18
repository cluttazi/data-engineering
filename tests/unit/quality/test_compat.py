"""Contract compatibility rule tests."""

from __future__ import annotations

from typing import Any

from quality.contracts.compat import check_all, check_compatibility
from quality.contracts.models import Contract


def _contract(version: int, fields: list[dict[str, Any]]) -> Contract:
    return Contract.model_validate(
        {
            "contract": "things",
            "version": version,
            "owner": "team@bank.example",
            "description": "test",
            "primary_key": ["thing_id"],
            "event_time_field": "updated_at",
            "sla": {"freshness_hours": 24},
            "fields": [
                {"name": "thing_id", "type": "string", "nullable": False},
                {"name": "updated_at", "type": "timestamp", "nullable": False},
                *fields,
            ],
        }
    )


def test_additive_nullable_field_is_compatible() -> None:
    old = _contract(1, [])
    new = _contract(2, [{"name": "note", "type": "string", "nullable": True}])
    assert check_compatibility(old, new) == []


def test_new_required_field_is_breaking() -> None:
    old = _contract(1, [])
    new = _contract(2, [{"name": "note", "type": "string", "nullable": False}])
    assert any("must be nullable" in b for b in check_compatibility(old, new))


def test_removed_field_is_breaking() -> None:
    old = _contract(1, [{"name": "note", "type": "string", "nullable": True}])
    new = _contract(2, [])
    assert any("field removed: note" in b for b in check_compatibility(old, new))


def test_type_change_is_breaking() -> None:
    old = _contract(1, [{"name": "amount", "type": "decimal(18,2)", "nullable": True}])
    new = _contract(2, [{"name": "amount", "type": "double", "nullable": True}])
    assert any("type changed" in b for b in check_compatibility(old, new))


def test_nullability_tightening_is_breaking() -> None:
    old = _contract(1, [{"name": "note", "type": "string", "nullable": True}])
    new = _contract(2, [{"name": "note", "type": "string", "nullable": False}])
    assert any("tightened" in b for b in check_compatibility(old, new))


def test_enum_shrink_is_breaking() -> None:
    old = _contract(
        1, [{"name": "kind", "type": "string", "nullable": True, "allowed_values": ["a", "b"]}]
    )
    new = _contract(
        2, [{"name": "kind", "type": "string", "nullable": True, "allowed_values": ["a"]}]
    )
    assert any("lost enum values" in b for b in check_compatibility(old, new))


def test_repo_contract_lineages_are_compatible() -> None:
    findings = check_all()
    assert "customers v1->v2" in findings
    breaking = {pair: breaks for pair, breaks in findings.items() if breaks}
    assert breaking == {}, f"breaking contract changes on disk: {breaking}"
