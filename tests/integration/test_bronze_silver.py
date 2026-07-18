"""End-to-end bronze -> silver over simulator-generated data.

Asserts the properties the demo depends on: exact quarantine counts,
checkpoint idempotency, SCD2 chain shape, and merge idempotency.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from ingestion.cdc_simulator.simulator import SimulationConfig, SimulationSummary, run_simulation
from observability.metrics.writer import metrics_table_path
from pipelines.bronze.job import BronzeCounts, quarantine_table_path, run_bronze_stream
from pipelines.common.config import LakehouseConfig
from pipelines.silver.job import SilverEntityResult, run_silver, silver_table_path
from tests.conftest import make_config

pytestmark = [pytest.mark.spark, pytest.mark.integration]

PipelineRun = tuple[LakehouseConfig, SimulationSummary, BronzeCounts, list[SilverEntityResult]]

EVENTS = 400
SEED = 42
CORRUPT_PCT = 5.0


@pytest.fixture(scope="module")
def pipeline_run(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> PipelineRun:
    """One full simulate -> bronze -> silver run shared by the assertions."""
    config = make_config(tmp_path_factory.mktemp("bronze-silver"))
    summary = run_simulation(
        SimulationConfig(
            events=EVENTS,
            seed=SEED,
            corrupt_pct=CORRUPT_PCT,
            landing_dir=config.source.cdc_landing_dir,
        )
    )
    bronze_counts = run_bronze_stream(config, spark)
    silver_results = run_silver(config, spark)
    return config, summary, bronze_counts, silver_results


def test_bronze_reads_everything(pipeline_run: PipelineRun) -> None:
    _, _summary, bronze_counts, _ = pipeline_run
    assert bronze_counts.rows_read == EVENTS
    assert bronze_counts.rows_written + bronze_counts.rows_quarantined == EVENTS


def test_bronze_quarantine_catches_exactly_the_corrupt_events(
    pipeline_run: PipelineRun,
    spark: SparkSession,
) -> None:
    config, summary, bronze_counts, _ = pipeline_run
    assert bronze_counts.rows_quarantined == summary.corrupt_events
    quarantine = spark.read.format("delta").load(quarantine_table_path(config))
    assert quarantine.count() == summary.corrupt_events
    reasons = {r["error_reason"] for r in quarantine.select("error_reason").distinct().collect()}
    assert reasons <= {
        "unparseable_json",
        "corrupt_envelope",
        "missing_op",
        "missing_or_invalid_ts_ms",
    }


def test_bronze_rerun_is_noop(pipeline_run: PipelineRun, spark: SparkSession) -> None:
    config, _, _, _ = pipeline_run
    second = run_bronze_stream(config, spark)
    assert second.rows_read == 0
    assert second.rows_written == 0


def test_scd2_chains_are_well_formed(pipeline_run: PipelineRun, spark: SparkSession) -> None:
    config, _, _, _ = pipeline_run
    customers = spark.read.format("delta").load(silver_table_path(config, "customers"))

    # exactly one current version per business key
    per_key = customers.groupBy("business_key").agg(
        F.sum(F.col("is_current").cast("int")).alias("current_versions"),
        F.count("*").alias("versions"),
    )
    assert per_key.filter(F.col("current_versions") != 1).count() == 0

    # closed versions end exactly where the next begins (no gaps/overlaps)
    window = Window.partitionBy("business_key").orderBy("valid_from")
    chained = customers.withColumn("next_from", F.lead("valid_from").over(window))
    broken = chained.filter(
        F.col("next_from").isNotNull() & (F.col("valid_to") != F.col("next_from"))
    )
    assert broken.count() == 0


def test_scd2_versions_match_event_history(pipeline_run: PipelineRun, spark: SparkSession) -> None:
    """Every customer's version count equals its insert+update event count."""
    config, _, _, _ = pipeline_run
    events_path = config.storage.lakehouse_root / "bronze" / "customers_events"
    events = spark.read.format("delta").load(str(events_path))
    expected = (
        events.filter(F.col("op").isin("c", "u"))
        .withColumn("key", F.get_json_object("raw_value", "$.after.customer_id"))
        .groupBy("key")
        .count()
    )
    customers = spark.read.format("delta").load(silver_table_path(config, "customers"))
    actual = customers.groupBy("business_key").count()
    mismatched = expected.join(
        actual, expected["key"] == actual["business_key"], "full_outer"
    ).filter(F.coalesce(expected["count"], F.lit(0)) != F.coalesce(actual["count"], F.lit(0)))
    assert mismatched.count() == 0


def test_deleted_loans_have_closed_chains(pipeline_run: PipelineRun, spark: SparkSession) -> None:
    config, summary, _, _ = pipeline_run
    n_deletes = summary.by_entity_op.get(("loan_applications", "d"), 0)
    if n_deletes == 0:
        pytest.skip("this seed produced no deletes")
    loans = spark.read.format("delta").load(silver_table_path(config, "loan_applications"))
    deleted_keys = loans.filter(F.col("is_deleted")).select("business_key").distinct()
    assert deleted_keys.count() == n_deletes
    # a deleted key has no current version
    still_current = loans.join(deleted_keys, "business_key").filter(F.col("is_current"))
    assert still_current.count() == 0


def test_transactions_append_only_and_deduped(
    pipeline_run: PipelineRun, spark: SparkSession
) -> None:
    config, _summary, _, _ = pipeline_run
    txns = spark.read.format("delta").load(silver_table_path(config, "transactions"))
    assert txns.count() == txns.select("transaction_id").distinct().count()


def test_silver_rerun_is_idempotent(pipeline_run: PipelineRun, spark: SparkSession) -> None:
    config, _, _, _ = pipeline_run
    before = {
        entity.name: spark.read.format("delta").load(silver_table_path(config, entity.name)).count()
        for entity in config.entities
    }
    run_silver(config, spark)
    after = {
        entity.name: spark.read.format("delta").load(silver_table_path(config, entity.name)).count()
        for entity in config.entities
    }
    assert before == after


def test_metrics_rows_recorded(pipeline_run: PipelineRun, spark: SparkSession) -> None:
    config, _, _, _ = pipeline_run
    metrics = spark.read.format("delta").load(str(metrics_table_path(config)))
    steps = {r["step"] for r in metrics.select("step").distinct().collect()}
    assert any(step.startswith("cdc_stream") for step in steps)
    assert any(step.startswith("historize") for step in steps)
    assert metrics.filter(F.col("status") == "failed").count() == 0
