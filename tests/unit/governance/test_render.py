"""Governance render tests: derived artifacts reflect their sources of truth."""

from __future__ import annotations

from governance.unity_catalog.render import (
    GENERATED_DIR,
    render_access_matrix,
    render_pii_tag_sql,
)
from quality.contracts.loader import load_contracts


def test_pii_sql_covers_every_contract_pii_field() -> None:
    sql = render_pii_tag_sql()
    for name, contract in load_contracts().items():
        for field in contract.pii_fields:
            assert f"silver.banking.{name} ALTER COLUMN {field.name}" in sql
            assert f"'pii_category' = '{field.pii_category}'" in sql
    assert "GENERATED" in sql  # do-not-edit marker present


def test_access_matrix_least_privilege_shape() -> None:
    matrix = render_access_matrix()
    # analysts appear, but never with MODIFY anywhere
    analyst_rows = [line for line in matrix.splitlines() if line.startswith("| analysts")]
    assert analyst_rows and "MODIFY" not in analyst_rows[0]
    # ingest service principal has no gold access
    ingest_rows = [line for line in matrix.splitlines() if line.startswith("| svc-ingest")]
    assert ingest_rows
    cells = [c.strip() for c in ingest_rows[0].split("|")]
    # securables sorted: bronze, gold, silver — ingest gets bronze only
    assert cells[4] == "—" and cells[5] == "—"


def test_committed_artifacts_match_render() -> None:
    """CI-style drift check: regenerating must produce what is committed."""
    committed_sql = (GENERATED_DIR / "pii_tags.sql").read_text(encoding="utf-8")
    committed_matrix = (GENERATED_DIR / "access_matrix.md").read_text(encoding="utf-8")
    assert committed_sql == render_pii_tag_sql()
    assert committed_matrix == render_access_matrix()
