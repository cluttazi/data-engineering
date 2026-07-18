"""Bronze CDC ingestion: Structured Streaming from the landing zone (or Kafka)
into per-entity Delta tables, with a quarantine split for broken events.

Runs with ``trigger(availableNow=True)``: a real streaming query with real
checkpoints that drains everything currently available and terminates —
demo-friendly, rerun-safe (already-processed files/offsets are never read
twice), and identical in code to an always-on deployment where only the
trigger changes.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from observability.metrics.writer import current_run_id, track_step
from pipelines.bronze.sources import build_change_event_stream, resolve_mode
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark

logger = logging.getLogger(__name__)

# Envelope metadata only — payloads (before/after) stay as raw JSON in bronze;
# business typing is silver's job, driven by the data contracts.
ENVELOPE_SCHEMA = StructType(
    [
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField(
            "source",
            StructType(
                [
                    StructField("table", StringType()),
                    StructField("lsn", LongType()),
                    StructField("connector", StringType()),
                    StructField("db", StringType()),
                ]
            ),
        ),
        StructField("_corrupt_record", StringType()),
    ]
)

VALID_OPS = ("c", "u", "d", "r")  # r = Debezium snapshot read


@dataclass
class BronzeCounts:
    """Per-run write counts, filled in by the foreachBatch callback."""

    rows_read: int = 0
    rows_quarantined: int = 0
    rows_by_entity: dict[str, int] = field(default_factory=dict)

    @property
    def rows_written(self) -> int:
        return sum(self.rows_by_entity.values())


def events_table_path(config: LakehouseConfig, entity: str) -> str:
    return str(config.storage.lakehouse_root / "bronze" / f"{entity}_events")


def quarantine_table_path(config: LakehouseConfig) -> str:
    return str(config.storage.lakehouse_root / "bronze" / "quarantine")


def parse_envelope(raw: DataFrame) -> DataFrame:
    """Parse envelope metadata permissively and classify each event.

    PERMISSIVE mode + a corrupt-record column means broken lines *flow* with
    a reason instead of failing the query (ANSI mode stays on globally).
    """
    parsed = raw.withColumn(
        "envelope",
        F.from_json(
            F.col("raw_value"),
            ENVELOPE_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    return parsed.withColumn(
        "error_reason",
        F.when(F.col("envelope").isNull(), F.lit("unparseable_json"))
        .when(F.col("envelope._corrupt_record").isNotNull(), F.lit("corrupt_envelope"))
        .when(F.col("envelope.op").isNull(), F.lit("missing_op"))
        .when(~F.col("envelope.op").isin(*VALID_OPS), F.lit("unknown_op"))
        .when(F.col("envelope.ts_ms").isNull(), F.lit("missing_or_invalid_ts_ms"))
        .when(F.col("envelope.source.table").isNull(), F.lit("missing_source_table"))
        .otherwise(F.lit(None).cast("string")),
    )


def _write_batch(
    batch: DataFrame,
    entities: list[str],
    config: LakehouseConfig,
    run_id: str,
    counts: BronzeCounts,
) -> None:
    """foreachBatch body: split valid/quarantine and append to Delta.

    One micro-batch writes several tables; the streaming checkpoint makes the
    *source* exactly-once and the writes idempotent-enough for the demo scale
    (a mid-batch crash can duplicate appends — acceptable for raw bronze,
    deduplicated in silver; a production variant would use txnAppId).
    """
    classified = parse_envelope(batch).persist()
    try:
        counts.rows_read += classified.count()

        quarantined = classified.filter(F.col("error_reason").isNotNull()).select(
            F.col("entity"),
            F.col("raw_value").alias("raw_line"),
            F.col("error_reason"),
            F.col("source_ref"),
            F.lit(run_id).alias("run_id"),
            F.current_timestamp().alias("ingest_ts"),
            F.current_date().alias("ingest_date"),
        )
        n_quarantined = quarantined.count()
        if n_quarantined:
            (
                quarantined.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingest_date")
                .save(quarantine_table_path(config))
            )
            counts.rows_quarantined += n_quarantined

        valid = classified.filter(F.col("error_reason").isNull())
        for entity in entities:
            entity_df = valid.filter(F.col("entity") == entity).select(
                F.col("event_key"),
                F.col("raw_value"),
                F.col("envelope.op").alias("op"),
                F.col("envelope.ts_ms").alias("ts_ms"),
                F.col("envelope.source.lsn").alias("lsn"),
                F.col("source_ref"),
                F.lit(run_id).alias("run_id"),
                F.current_timestamp().alias("ingest_ts"),
                F.current_date().alias("ingest_date"),
            )
            n_rows = entity_df.count()
            if n_rows:
                (
                    entity_df.write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")  # envelope evolution lands additively
                    .partitionBy("ingest_date")
                    .save(events_table_path(config, entity))
                )
                counts.rows_by_entity[entity] = counts.rows_by_entity.get(entity, 0) + n_rows
    finally:
        classified.unpersist()


def run_bronze_stream(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> BronzeCounts:
    """Drain all available change events into bronze; returns write counts."""
    config = config or load_config()
    mode = resolve_mode(config)
    spark = spark or get_spark("bronze-cdc", config, with_kafka=(mode == "kafka"))
    run_id = current_run_id()
    entities = [e.name for e in config.entities]
    counts = BronzeCounts()

    with track_step(
        spark, config, run_id=run_id, pipeline="bronze", step=f"cdc_stream[{mode}]", layer="bronze"
    ) as metric:
        stream = build_change_event_stream(spark, config, mode)
        query = (
            stream.writeStream.foreachBatch(
                lambda batch, _epoch: _write_batch(batch, entities, config, run_id, counts)
            )
            .option("checkpointLocation", str(config.storage.checkpoints_dir / "bronze_cdc"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        metric.rows_read = counts.rows_read
        metric.rows_written = counts.rows_written
        metric.rows_quarantined = counts.rows_quarantined
        metric.extra = {f"rows[{k}]": str(v) for k, v in sorted(counts.rows_by_entity.items())}
    return counts


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    counts = run_bronze_stream()
    print(
        f"bronze: read={counts.rows_read} written={counts.rows_written} "
        f"quarantined={counts.rows_quarantined}"
    )
    for entity, n_rows in sorted(counts.rows_by_entity.items()):
        print(f"  {entity:18s} +{n_rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
