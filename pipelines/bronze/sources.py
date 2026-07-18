"""Dual-mode change-event source: one envelope, two transports.

``build_change_event_stream`` returns a streaming DataFrame with the same
normalized columns regardless of transport::

    entity      STRING  -- source table (from file path or Kafka topic)
    event_key   STRING  -- Kafka message key; null in file mode
    raw_value   STRING  -- the untouched serialized envelope line
    source_ref  STRING  -- file path or topic/partition/offset

Everything downstream (envelope parsing, quarantine split, Delta writes) is
transport-agnostic — swapping the local NDJSON landing zone for a real
Debezium->Kafka feed changes *configuration*, not pipeline code. This is the
core design decision of the ingestion layer:
see docs/adr/006-file-vs-kafka-dual-mode-transport.md.

File mode reads with ``format("text")`` rather than ``format("json")`` on
purpose: the JSON reader would silently null out malformed lines, while the
text reader delivers them verbatim so the quarantine split can catch and
explain them.
"""

from __future__ import annotations

import logging
import socket

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.config import LakehouseConfig, SourceMode

logger = logging.getLogger(__name__)


def resolve_mode(config: LakehouseConfig, probe_timeout_s: float = 1.0) -> SourceMode:
    """Resolve ``auto`` to a concrete transport with a cheap TCP probe."""
    mode = config.source.mode
    if mode != "auto":
        return mode
    host, _, port = config.source.kafka.bootstrap_servers.partition(":")
    try:
        with socket.create_connection((host, int(port or "9092")), timeout=probe_timeout_s):
            logger.info("auto mode: broker reachable at %s -> kafka", host)
            return "kafka"
    except OSError:
        logger.warning(
            "auto mode: no broker at %s -> falling back to file transport",
            config.source.kafka.bootstrap_servers,
        )
        return "file"


def build_change_event_stream(
    spark: SparkSession, config: LakehouseConfig, mode: SourceMode
) -> DataFrame:
    """Streaming DataFrame of raw change events in the normalized envelope."""
    if mode == "file":
        landing = config.source.cdc_landing_dir
        return (
            spark.readStream.format("text")
            .option("pathGlobFilter", "*.ndjson")
            .load(f"{landing}/*")
            .select(
                F.regexp_extract(F.col("_metadata.file_path"), r"/cdc/([^/]+)/", 1).alias("entity"),
                F.lit(None).cast("string").alias("event_key"),
                F.col("value").alias("raw_value"),
                F.col("_metadata.file_path").alias("source_ref"),
            )
        )
    if mode == "kafka":
        kafka = config.source.kafka
        prefix = kafka.topic_prefix
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
            .option("subscribePattern", prefix.replace(".", r"\.") + r"\..+")
            .option("startingOffsets", kafka.starting_offsets)
            .load()
            .select(
                F.expr(f"substring(topic, {len(prefix) + 2})").alias("entity"),
                F.col("key").cast("string").alias("event_key"),
                F.col("value").cast("string").alias("raw_value"),
                F.concat_ws(
                    ":", F.col("topic"), F.col("partition").cast("string"), F.col("offset")
                ).alias("source_ref"),
            )
        )
    raise ValueError(f"mode must be resolved to file|kafka before building a stream, got {mode!r}")
