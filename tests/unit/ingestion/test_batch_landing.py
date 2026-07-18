"""Unit tests for the batch landing generator."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.batch_landing.generator import generate_all


def test_generates_all_three_formats(tmp_path: Path) -> None:
    files = generate_all(tmp_path, seed=42)
    suffixes = {generated.path.suffix for generated in files}
    assert suffixes == {".csv", ".json", ".parquet"}
    for generated in files:
        assert generated.path.exists()
        assert generated.rows > 0


def test_branches_resend_file_is_byte_identical(tmp_path: Path) -> None:
    files = generate_all(tmp_path, seed=42)
    branch_files = sorted(f.path for f in files if f.dataset == "branches")
    assert len(branch_files) == 2
    assert branch_files[0].read_bytes() == branch_files[1].read_bytes()
    assert branch_files[0].name != branch_files[1].name


def test_branches_csv_shape(tmp_path: Path) -> None:
    files = generate_all(tmp_path, seed=42)
    primary = next(f.path for f in files if f.dataset == "branches" and "resend" not in f.path.name)
    with primary.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 25
    assert set(rows[0]) == {"branch_id", "branch_name", "region", "opened_date", "business_date"}


def test_products_json_is_valid(tmp_path: Path) -> None:
    files = generate_all(tmp_path, seed=42)
    products_path = next(f.path for f in files if f.dataset == "products")
    products = json.loads(products_path.read_text(encoding="utf-8"))
    assert {p["category"] for p in products} == {"deposit", "lending", "card"}


def test_fx_parquet_readable(tmp_path: Path) -> None:
    files = generate_all(tmp_path, seed=42)
    fx_path = next(f.path for f in files if f.dataset == "fx_rates")
    table = pq.read_table(fx_path)  # type: ignore[no-untyped-call]  # pyarrow io is unannotated
    assert table.num_rows == 5
    assert set(table.column_names) == {"base_currency", "quote_currency", "rate", "business_date"}


def test_deterministic_output(tmp_path: Path) -> None:
    files_a = generate_all(tmp_path / "a", seed=42)
    files_b = generate_all(tmp_path / "b", seed=42)
    for a, b in zip(files_a, files_b, strict=True):
        assert a.path.read_bytes() == b.path.read_bytes()
