"""Spark session factory for all local pipelines.

Centralizes the settings that would otherwise drift between jobs:

* Delta Lake wiring via ``configure_spark_with_delta_pip`` — the Delta jars
  ship inside the ``delta-spark`` wheel, so no network resolution happens for
  the default (file-mode) demo.
* UTC session timezone: Debezium ``ts_ms`` is epoch millis; SCD2
  ``valid_from``/``valid_to`` must not depend on the host timezone.
* ANSI mode stays ON (the Spark 4 default). Dirty CDC data is handled with
  ``try_cast``/permissive JSON parsing at the edges, not by globally
  disabling correctness — see docs/adr/007-ansi-mode-on.md.
* The Kafka connector package is added only when the transport is kafka, so
  file-mode runs never touch Ivy/Maven.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from pipelines.common.config import LakehouseConfig

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


def get_spark(
    app_name: str,
    config: LakehouseConfig,
    *,
    with_kafka: bool = False,
) -> SparkSession:
    """Build (or reuse) the local SparkSession with Delta enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.session.timeZone", config.spark.session_timezone)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
    )
    extra_packages = [KAFKA_PACKAGE] if with_kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
