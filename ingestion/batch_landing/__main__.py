"""CLI entry point: ``python -m ingestion.batch_landing``."""

from __future__ import annotations

import argparse
import sys

from ingestion.batch_landing.generator import generate_all
from pipelines.common.config import load_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="batch_landing",
        description="Drop reference-data files (CSV/JSON/Parquet) into the batch landing zone.",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    landing_dir = load_config().source.batch_landing_dir
    files = generate_all(landing_dir, seed=args.seed)
    print(f"batch_landing: wrote {len(files)} files under {landing_dir}")
    for generated in files:
        print(f"  {generated.dataset:10s} {generated.path.name} ({generated.rows} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
