"""Debezium-style change-event envelope.

The shape matches what a Debezium Postgres connector produces with the JSON
converter and ``schemas.enable=false`` (payload only): ``before``/``after`` row
images, an ``op`` code, the event ``ts_ms``, and a ``source`` block identifying
the origin table and log position. Downstream bronze parsing is written against
this schema, so a real connector could replace the simulator without touching
the pipelines.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

Op = Literal["c", "u", "d"]

CONNECTOR_NAME = "banking"
SOURCE_DB = "banking"
SOURCE_SCHEMA = "banking"


@dataclass(frozen=True)
class ChangeEvent:
    """One change event, ready for a sink.

    ``key`` is the primary-key value (Kafka message key / file grouping);
    ``line`` is the serialized envelope — or a deliberately malformed string
    when the simulator injects corruption.
    """

    entity: str
    key: str
    line: str
    ts_ms: int


def build_envelope(
    *,
    entity: str,
    op: Op,
    ts_ms: int,
    lsn: int,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
) -> dict[str, Any]:
    """Assemble the Debezium payload for one row-level change."""
    return {
        "before": before,
        "after": after,
        "source": {
            "version": "2.7.0.Final",
            "connector": "postgresql",
            "name": CONNECTOR_NAME,
            "ts_ms": ts_ms,
            "db": SOURCE_DB,
            "schema": SOURCE_SCHEMA,
            "table": entity,
            "lsn": lsn,
        },
        "op": op,
        "ts_ms": ts_ms,
    }


def serialize(envelope: dict[str, Any]) -> str:
    """Canonical single-line JSON; sorted keys keep output byte-reproducible."""
    return json.dumps(envelope, sort_keys=True, separators=(",", ":"))
