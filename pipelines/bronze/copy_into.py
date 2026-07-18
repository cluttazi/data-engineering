"""COPY INTO-semantics batch loader for the file landing zone.

Databricks' ``COPY INTO`` guarantees each file loads exactly once by tracking
loaded files in table metadata. Locally we make that ledger explicit: a Delta
table (``bronze/ops/file_ledger``) records every ingested file path, and each
run loads only the set difference. Re-running is a no-op; a *resent* file
under a new name is loaded (new path = new file) and left for silver to
deduplicate — the same behavior COPY INTO exhibits.

Why not model Auto Loader instead? See docs/adr/001-copy-into-vs-auto-loader.md:
these are low-frequency reference feeds where an explicit, auditable ledger
beats notification infrastructure.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from observability.metrics.writer import current_run_id, track_step
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark


@dataclass(frozen=True)
class BatchDataset:
    name: str
    file_format: str  # csv | json | parquet
    reader_options: dict[str, str]


BATCH_DATASETS = [
    BatchDataset("branches", "csv", {"header": "true"}),
    BatchDataset("products", "json", {"multiLine": "true"}),
    BatchDataset("fx_rates", "parquet", {}),
]

LEDGER_SCHEMA = StructType(
    [
        StructField("file_path", StringType(), nullable=False),
        StructField("dataset", StringType(), nullable=False),
        StructField("rows_loaded", LongType(), nullable=False),
        StructField("run_id", StringType(), nullable=False),
        StructField("loaded_at", TimestampType(), nullable=False),
    ]
)


def ledger_path(config: LakehouseConfig) -> str:
    return str(config.storage.lakehouse_root / "bronze" / "ops" / "file_ledger")


def batch_table_path(config: LakehouseConfig, dataset: str) -> str:
    return str(config.storage.lakehouse_root / "bronze" / "batch" / dataset)


def _already_loaded(spark: SparkSession, config: LakehouseConfig, dataset: str) -> set[str]:
    try:
        ledger = spark.read.format("delta").load(ledger_path(config))
    except Exception:  # first run: ledger doesn't exist yet
        return set()
    rows = ledger.filter(F.col("dataset") == dataset).select("file_path").collect()
    return {r["file_path"] for r in rows}


def _read_files(spark: SparkSession, dataset: BatchDataset, paths: list[str]) -> DataFrame:
    reader = spark.read.format(dataset.file_format)
    for key, value in dataset.reader_options.items():
        reader = reader.option(key, value)
    df = reader.load(paths)
    return df.withColumn("_source_file", F.col("_metadata.file_path"))


def run_copy_into(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> dict[str, int]:
    """Load new landing files exactly once; returns rows loaded per dataset."""
    config = config or load_config()
    spark = spark or get_spark("bronze-copy-into", config)
    run_id = current_run_id()
    loaded: dict[str, int] = {}

    with track_step(
        spark, config, run_id=run_id, pipeline="bronze", step="copy_into", layer="bronze"
    ) as metric:
        for dataset in BATCH_DATASETS:
            landing = config.source.batch_landing_dir / dataset.name
            if not landing.exists():
                continue
            all_files = sorted(str(p) for p in landing.iterdir() if p.is_file())
            already = _already_loaded(spark, config, dataset.name)
            new_files = [p for p in all_files if p not in already]
            if not new_files:
                loaded[dataset.name] = 0
                continue

            df = _read_files(spark, dataset, new_files)
            df = df.withColumn("_run_id", F.lit(run_id)).withColumn(
                "_ingest_ts", F.current_timestamp()
            )
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(batch_table_path(config, dataset.name))
            )

            per_file = (
                df.groupBy("_source_file")
                .count()
                .select(
                    F.col("_source_file").alias("file_path"),
                    F.lit(dataset.name).alias("dataset"),
                    F.col("count").cast("long").alias("rows_loaded"),
                    F.lit(run_id).alias("run_id"),
                    F.current_timestamp().alias("loaded_at"),
                )
            )
            per_file.write.format("delta").mode("append").save(ledger_path(config))
            loaded[dataset.name] = sum(r["rows_loaded"] for r in per_file.collect())

        metric.rows_written = sum(loaded.values())
        metric.extra = {f"rows[{k}]": str(v) for k, v in sorted(loaded.items())}
    return loaded


def main() -> int:
    loaded = run_copy_into()
    total = sum(loaded.values())
    print(f"copy_into: loaded {total} rows from new files")
    for dataset, n_rows in sorted(loaded.items()):
        print(f"  {dataset:10s} +{n_rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
