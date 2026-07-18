"""Tiny Parquet fixtures so CI can `dbt build` with no Spark and no pipelines.

Writes a minimal-but-referentially-consistent copy of every source table the
dbt project reads, in the exact export layout
(``<out>/exports/silver/<table>/*.parquet``). CI points dbt at it via::

    uv run python scripts/make_dbt_fixtures.py --out .ci_fixtures
    LAKEHOUSE_EXPORTS=.ci_fixtures/exports \
    LAKEHOUSE_WAREHOUSE=.ci_fixtures/warehouse.duckdb \
    uv run dbt build --project-dir transform/dbt_project --profiles-dir transform/dbt_project

Keep the columns in lockstep with what the silver export produces — the
integration tests cover the real path; this covers the dbt SQL hermetically.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TS = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
TS2 = datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC)


def _write(out_root: Path, table: str, rows: list[dict[str, object]]) -> None:
    out_dir = out_root / "exports" / "silver" / table
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(  # type: ignore[no-untyped-call]  # pyarrow io is unannotated
        pa.Table.from_pylist(rows), out_dir / "part-0.parquet"
    )


def build_fixtures(out_root: Path) -> None:
    _write(
        out_root,
        "customers",
        [
            {
                "business_key": "CUST-1",
                "customer_id": "CUST-1",
                "full_name": "Taro Fixture",
                "email": "taro@example.com",
                "phone": None,
                "address": None,
                "segment": "retail",
                "risk_rating": "low",
                "preferred_language": None,
                "created_at": TS,
                "updated_at": TS,
                "valid_from": TS,
                "valid_to": TS2,
                "is_current": False,
                "is_deleted": False,
                "_run_id": "fixture",
            },
            {
                "business_key": "CUST-1",
                "customer_id": "CUST-1",
                "full_name": "Taro Fixture",
                "email": "taro@example.com",
                "phone": None,
                "address": None,
                "segment": "premium",
                "risk_rating": "low",
                "preferred_language": "ja",
                "created_at": TS,
                "updated_at": TS2,
                "valid_from": TS2,
                "valid_to": None,
                "is_current": True,
                "is_deleted": False,
                "_run_id": "fixture",
            },
            {
                "business_key": "CUST-2",
                "customer_id": "CUST-2",
                "full_name": "Hanako Fixture",
                "email": "hanako@example.com",
                "phone": None,
                "address": None,
                "segment": "private",
                "risk_rating": "high",
                "preferred_language": None,
                "created_at": TS,
                "updated_at": TS,
                "valid_from": TS,
                "valid_to": None,
                "is_current": True,
                "is_deleted": False,
                "_run_id": "fixture",
            },
        ],
    )
    _write(
        out_root,
        "accounts",
        [
            {
                "business_key": "ACCT-1",
                "account_id": "ACCT-1",
                "customer_id": "CUST-1",
                "account_type": "checking",
                "currency": "JPY",
                "balance": Decimal("150000.00"),
                "status": "active",
                "opened_at": TS,
                "updated_at": TS,
                "valid_from": TS,
                "valid_to": None,
                "is_current": True,
                "is_deleted": False,
                "_run_id": "fixture",
            },
            {
                "business_key": "ACCT-2",
                "account_id": "ACCT-2",
                "customer_id": "CUST-2",
                "account_type": "savings",
                "currency": "USD",
                "balance": Decimal("980.55"),
                "status": "active",
                "opened_at": TS,
                "updated_at": TS,
                "valid_from": TS,
                "valid_to": None,
                "is_current": True,
                "is_deleted": False,
                "_run_id": "fixture",
            },
        ],
    )
    _write(
        out_root,
        "transactions",
        [
            {
                "transaction_id": f"TXN-{i}",
                "account_id": "ACCT-1" if i % 2 else "ACCT-2",
                "amount": Decimal(f"{1000 + i}.00"),
                "currency": "JPY" if i % 2 else "USD",
                "txn_type": "card_payment",
                "counterparty": "Fixture Store",
                "channel": "mobile",
                "status": "posted",
                "booked_at": TS,
                "_run_id": "fixture",
            }
            for i in range(1, 5)
        ],
    )
    _write(
        out_root,
        "loan_applications",
        [
            {
                "business_key": "LOAN-1",
                "application_id": "LOAN-1",
                "customer_id": "CUST-1",
                "product": "personal_loan",
                "amount": Decimal("500000.00"),
                "term_months": 24,
                "status": "approved",
                "credit_score": 720,
                "submitted_at": TS,
                "updated_at": TS2,
                "valid_from": TS2,
                "valid_to": None,
                "is_current": True,
                "is_deleted": False,
                "_run_id": "fixture",
            },
            {
                "business_key": "LOAN-2",
                "application_id": "LOAN-2",
                "customer_id": "CUST-2",
                "product": "mortgage",
                "amount": Decimal("25000000.00"),
                "term_months": 360,
                "status": "rejected",
                "credit_score": 480,
                "submitted_at": TS,
                "updated_at": TS2,
                "valid_from": TS2,
                "valid_to": None,
                "is_current": True,
                "is_deleted": False,
                "_run_id": "fixture",
            },
        ],
    )
    _write(
        out_root,
        "branches",
        [
            {
                "branch_id": "BR-001",
                "branch_name": "Fixture Branch",
                "region": "kanto",
                "opened_date": "2001-04-01",
                "business_date": "2026-01-01",
                "_source_file": "fixture.csv",
                "_run_id": "fixture",
                "_ingest_ts": TS,
            },
        ],
    )
    _write(
        out_root,
        "products",
        [
            {
                "product_code": "CHK-STD",
                "name": "Standard Checking",
                "category": "deposit",
                "annual_rate_pct": 0.02,
                "active": True,
                "business_date": "2026-01-01",
                "_source_file": "fixture.json",
                "_run_id": "fixture",
                "_ingest_ts": TS,
            },
        ],
    )
    _write(
        out_root,
        "fx_rates",
        [
            {
                "base_currency": "JPY",
                "quote_currency": quote,
                "rate": rate,
                "business_date": "2026-01-01",
                "_source_file": "fixture.parquet",
                "_run_id": "fixture",
                "_ingest_ts": TS,
            }
            for quote, rate in [("USD", 0.0064), ("EUR", 0.0059)]
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    build_fixtures(args.out)
    print(f"dbt fixtures written under {args.out}/exports/silver")
    return 0


if __name__ == "__main__":
    sys.exit(main())
