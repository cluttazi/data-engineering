"""SCD Type 2 historization via Delta ``MERGE INTO``.

Approach: **recompute version chains from full bronze history, then merge.**

Each entity's contract-valid events are ordered per business key by
``(ts_ms, lsn)`` and turned into version rows: ``valid_from`` is the event
time, ``valid_to`` is the next event's time (null while open), and
``is_current`` marks the last version — unless the chain ends in a delete,
which closes it (``is_deleted`` on the final version records the erasure).

The computed rows are then MERGEd into the silver table keyed on
``(business_key, valid_from)``: existing versions get their ``valid_to`` /
``is_current`` refreshed, new versions are inserted. The whole operation is
idempotent — replaying the same bronze events is a no-op, and late events
slot into the chain and repair ``valid_to`` on the previously-open version.

Trade-off (docs/adr/002-scd2-merge-vs-overwrite.md): recompute-then-merge is
O(history) per run, which is the honest choice at demo/reference scale and
keeps the logic testable in one pass. The Databricks-scale variant keeps the
same MERGE and narrows the recompute window to keys touched since the last
run; ``overwrite`` was rejected because it destroys the audit history that
regulated environments exist to keep.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from quality.contracts.models import Contract


def build_version_rows(events: DataFrame, contract: Contract) -> DataFrame:
    """Turn ordered change events into SCD2 version rows.

    ``events`` must carry typed ``after``/``before`` structs plus
    ``op``/``ts_ms``/``lsn`` (contract-valid rows from enforcement).
    """
    key = contract.primary_key[0]
    business_key = F.coalesce(F.col(f"after.{key}"), F.col(f"before.{key}"))
    window = Window.partitionBy("business_key").orderBy("ts_ms", "lsn")

    ordered = (
        events.withColumn("business_key", business_key)
        # Exact-duplicate delivery (kafka at-least-once, file resends): one
        # event per (key, lsn, op) survives.
        .dropDuplicates(["business_key", "lsn", "op"])
        .withColumn("next_op", F.lead("op").over(window))
        .withColumn("next_ts_ms", F.lead("ts_ms").over(window))
    )

    versions = ordered.filter(F.col("op").isin("c", "u", "r"))
    return (
        versions.select(
            F.col("business_key"),
            F.col("after.*"),
            F.timestamp_millis(F.col("ts_ms")).alias("valid_from"),
            F.timestamp_millis(F.col("next_ts_ms")).alias("valid_to"),
            (F.col("next_ts_ms").isNull()).alias("is_current"),
            (F.col("next_op") == "d").alias("closed_by_delete"),
            F.col("run_id").alias("_run_id"),
        )
        .withColumn("is_deleted", F.coalesce(F.col("closed_by_delete"), F.lit(False)))
        .drop("closed_by_delete")
    )


def merge_scd2(
    spark: SparkSession, version_rows: DataFrame, target_path: str, contract: Contract
) -> int:
    """MERGE computed version rows into the silver SCD2 table; returns row count.

    Match on ``(business_key, valid_from)`` — a version identity, not a row
    identity — so re-merging refreshed chains updates closure columns in
    place and never duplicates versions.
    """
    n_versions = version_rows.count()
    if not DeltaTable.isDeltaTable(spark, target_path):
        (version_rows.write.format("delta").mode("overwrite").save(target_path))
        return n_versions

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(
            version_rows.alias("s"),
            "t.business_key = s.business_key AND t.valid_from = s.valid_from",
        )
        .whenMatchedUpdate(
            set={
                "valid_to": "s.valid_to",
                "is_current": "s.is_current",
                "is_deleted": "s.is_deleted",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return n_versions


def append_only_merge(
    spark: SparkSession, rows: DataFrame, target_path: str, contract: Contract
) -> int:
    """Idempotent append for immutable facts: insert-if-absent by primary key.

    ``whenNotMatchedInsertAll`` with no matched clause turns replayed events
    into no-ops — the MERGE equivalent of exactly-once for append-only data.
    """
    key = contract.primary_key[0]
    deduped = rows.dropDuplicates([key])
    n_rows = deduped.count()
    if not DeltaTable.isDeltaTable(spark, target_path):
        deduped.write.format("delta").mode("overwrite").save(target_path)
        return n_rows

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(deduped.alias("s"), f"t.{key} = s.{key}")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return n_rows
