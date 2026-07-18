"""Silver pipeline: bronze events -> contract-enforced, historized tables.

Per entity: read bronze, parse payloads against the contract, enforce rules
(violations -> ``silver/quarantine``), then historize — SCD2 merge for
mutable entities, insert-if-absent merge for append-only facts. The whole
job is idempotent: re-running against unchanged bronze is a no-op.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from observability.metrics.writer import current_run_id, track_step
from pipelines.common.config import EntityConfig, LakehouseConfig, load_config
from pipelines.common.session import get_spark
from pipelines.silver.enforcement import enforce_contract, parse_payloads
from pipelines.silver.scd2 import append_only_merge, build_version_rows, merge_scd2
from quality.contracts.loader import load_contracts
from quality.contracts.models import Contract


@dataclass
class SilverEntityResult:
    entity: str
    rows_read: int
    rows_written: int
    rows_quarantined: int


def silver_table_path(config: LakehouseConfig, entity: str) -> str:
    return str(config.storage.lakehouse_root / "silver" / entity)


def silver_quarantine_path(config: LakehouseConfig) -> str:
    return str(config.storage.lakehouse_root / "silver" / "quarantine")


def _process_entity(
    spark: SparkSession,
    config: LakehouseConfig,
    entity: EntityConfig,
    contract: Contract,
    run_id: str,
) -> SilverEntityResult:
    bronze_path = config.storage.lakehouse_root / "bronze" / f"{entity.name}_events"
    events = spark.read.format("delta").load(str(bronze_path))
    rows_read = events.count()

    parsed = parse_payloads(events, contract)
    result = enforce_contract(parsed, contract)

    quarantined = result.quarantined.count()
    if quarantined:
        (
            result.quarantined.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("quarantine_date")
            .save(silver_quarantine_path(config))
        )

    target = silver_table_path(config, entity.name)
    if entity.scd == "type2":
        version_rows = build_version_rows(result.valid, contract)
        rows_written = merge_scd2(spark, version_rows, target, contract)
    else:
        fact_rows = result.valid.filter(F.col("op").isin("c", "r")).select(
            "after.*", F.col("run_id").alias("_run_id")
        )
        rows_written = append_only_merge(spark, fact_rows, target, contract)

    return SilverEntityResult(entity.name, rows_read, rows_written, quarantined)


def run_silver(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> list[SilverEntityResult]:
    """Process every configured entity; returns per-entity results."""
    config = config or load_config()
    spark = spark or get_spark("silver", config)
    run_id = current_run_id()
    contracts = load_contracts()
    results: list[SilverEntityResult] = []

    for entity in config.entities:
        contract = contracts[entity.name]
        with track_step(
            spark,
            config,
            run_id=run_id,
            pipeline="silver",
            step=f"historize[{entity.name}]",
            layer="silver",
        ) as metric:
            result = _process_entity(spark, config, entity, contract, run_id)
            metric.rows_read = result.rows_read
            metric.rows_written = result.rows_written
            metric.rows_quarantined = result.rows_quarantined
            metric.extra = {"scd": entity.scd, "contract_version": str(contract.version)}
        results.append(result)
    return results


def main() -> int:
    results = run_silver()
    print("silver: contract enforcement + historization")
    for r in results:
        print(
            f"  {r.entity:18s} read={r.rows_read:5d} versions/rows={r.rows_written:5d} "
            f"quarantined={r.rows_quarantined}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
