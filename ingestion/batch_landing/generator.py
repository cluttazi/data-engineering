"""Deterministic generators for the batch landing zone.

Three datasets, three formats — deliberately heterogeneous:

* ``branches``  → CSV (the classic ops-team export), including a byte-identical
  *resend* file under a different name: the file ledger loads both (different
  paths), and silver's dedup collapses the duplicate rows.
* ``products``  → JSON (product catalog owned by an app team)
* ``fx_rates``  → Parquet (a well-behaved upstream), written via pyarrow
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

FX_BASE = "JPY"
FX_QUOTES = ["USD", "EUR", "GBP", "AUD", "SGD"]
BUSINESS_DATE = "2026-01-01"  # logical drop date; matches the CDC clock epoch


@dataclass(frozen=True)
class GeneratedFile:
    dataset: str
    path: Path
    rows: int


def generate_branches(landing_dir: Path, faker: Faker, rng: random.Random) -> list[GeneratedFile]:
    rows = [
        {
            "branch_id": f"BR-{i:03d}",
            "branch_name": f"{faker.city()} Branch",
            "region": rng.choice(["kanto", "kansai", "chubu", "kyushu", "tohoku"]),
            "opened_date": faker.date_between_dates(
                date_start=date(1990, 1, 1), date_end=date(2020, 12, 31)
            ).isoformat(),
            "business_date": BUSINESS_DATE,
        }
        for i in range(1, 26)
    ]
    out_dir = landing_dir / "branches"
    out_dir.mkdir(parents=True, exist_ok=True)
    primary = out_dir / f"branches_{BUSINESS_DATE}.csv"
    resend = out_dir / f"branches_{BUSINESS_DATE}_resend.csv"
    for path in (primary, resend):  # identical content: the classic double-send
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    return [
        GeneratedFile("branches", primary, len(rows)),
        GeneratedFile("branches", resend, len(rows)),
    ]


def generate_products(landing_dir: Path, rng: random.Random) -> list[GeneratedFile]:
    products = [
        {"product_code": "CHK-STD", "name": "Standard Checking", "category": "deposit"},
        {"product_code": "SAV-STD", "name": "Standard Savings", "category": "deposit"},
        {"product_code": "SAV-HI", "name": "High-Yield Savings", "category": "deposit"},
        {"product_code": "TD-12M", "name": "12M Term Deposit", "category": "deposit"},
        {"product_code": "LN-PERS", "name": "Personal Loan", "category": "lending"},
        {"product_code": "LN-MTG", "name": "Residential Mortgage", "category": "lending"},
        {"product_code": "LN-AUTO", "name": "Auto Loan", "category": "lending"},
        {"product_code": "CRD-STD", "name": "Standard Card", "category": "card"},
        {"product_code": "CRD-PLT", "name": "Platinum Card", "category": "card"},
    ]
    rows = [
        {
            **product,
            "annual_rate_pct": round(rng.uniform(0.01, 14.5), 2),
            "active": True,
            "business_date": BUSINESS_DATE,
        }
        for product in products
    ]
    out_dir = landing_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"products_{BUSINESS_DATE}.json"
    path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    return [GeneratedFile("products", path, len(rows))]


def generate_fx_rates(landing_dir: Path, rng: random.Random) -> list[GeneratedFile]:
    base_rates = {"USD": 0.0064, "EUR": 0.0059, "GBP": 0.0050, "AUD": 0.0099, "SGD": 0.0086}
    rows = [
        {
            "base_currency": FX_BASE,
            "quote_currency": quote,
            "rate": round(base_rates[quote] * rng.uniform(0.97, 1.03), 6),
            "business_date": BUSINESS_DATE,
        }
        for quote in FX_QUOTES
    ]
    out_dir = landing_dir / "fx_rates"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"fx_rates_{BUSINESS_DATE}.parquet"
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)  # type: ignore[no-untyped-call]  # pyarrow io is unannotated
    return [GeneratedFile("fx_rates", path, len(rows))]


def generate_all(landing_dir: Path, seed: int) -> list[GeneratedFile]:
    """Generate every batch dataset; deterministic for a given seed."""
    rng = random.Random(seed)
    faker = Faker()
    faker.seed_instance(seed)
    files: list[GeneratedFile] = []
    files += generate_branches(landing_dir, faker, rng)
    files += generate_products(landing_dir, rng)
    files += generate_fx_rates(landing_dir, rng)
    return files
