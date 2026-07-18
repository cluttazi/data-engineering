"""Publish silver (and batch reference) snapshots as Parquet for dbt/DuckDB.

dbt reads these exports instead of the Delta tables directly — an explicit
copy, chosen deliberately: it keeps the dbt project hermetic (CI builds it
from tiny committed fixtures without Spark), avoids depending on DuckDB's
delta extension being able to read Delta 4 tables written by MERGE-heavy
workloads, and mirrors the real pattern of publishing curated snapshots to a
consumption layer. Trade-offs in docs/adr/003-duckdb-parquet-export-for-dbt.md.
"""

from __future__ import annotations

import shutil
import sys

from pyspark.sql import SparkSession

from observability.metrics.writer import current_run_id, track_step
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark

BATCH_DATASETS = ["branches", "products", "fx_rates"]


def run_export(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> dict[str, int]:
    """Snapshot every silver entity + batch reference table to Parquet dirs."""
    config = config or load_config()
    spark = spark or get_spark("export-for-dbt", config)
    run_id = current_run_id()
    export_root = config.storage.exports_dir / "silver"
    exported: dict[str, int] = {}

    sources: dict[str, str] = {
        entity.name: str(config.storage.lakehouse_root / "silver" / entity.name)
        for entity in config.entities
    }
    for dataset in BATCH_DATASETS:
        sources[dataset] = str(config.storage.lakehouse_root / "bronze" / "batch" / dataset)

    with track_step(
        spark, config, run_id=run_id, pipeline="silver", step="export_for_dbt", layer="silver"
    ) as metric:
        for name, path in sources.items():
            df = spark.read.format("delta").load(path)
            out_dir = export_root / name
            if out_dir.exists():  # overwrite semantics: exports are snapshots
                shutil.rmtree(out_dir)
            df.coalesce(1).write.mode("overwrite").parquet(str(out_dir))
            exported[name] = df.count()
        metric.rows_written = sum(exported.values())
        metric.extra = {f"rows[{k}]": str(v) for k, v in sorted(exported.items())}
    return exported


def main() -> int:
    exported = run_export()
    print(f"export_for_dbt: {len(exported)} tables -> parquet")
    for name, n_rows in sorted(exported.items()):
        print(f"  {name:18s} {n_rows} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
