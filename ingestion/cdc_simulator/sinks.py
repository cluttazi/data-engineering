"""Event sinks: NDJSON files (default) and Kafka.

Both sinks receive the already-serialized line, so corrupt-event injection and
serialization policy stay in the simulator; a sink only decides *where* bytes
go. This is also why the file and kafka transports feed byte-identical payloads
into bronze — the pipeline can't tell them apart, by design.
"""

from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Protocol

from ingestion.cdc_simulator.envelope import ChangeEvent


class Sink(Protocol):
    """Destination for serialized change events."""

    def emit(self, event: ChangeEvent) -> None: ...

    def close(self) -> None: ...


class FileSink:
    """Writes NDJSON per entity under ``landing_dir``, rotating every N events.

    File names are sequential (``events_00001.ndjson``) so a seeded run always
    produces the same layout — the tests diff whole directories.
    """

    def __init__(self, landing_dir: Path, rotate_every: int = 500) -> None:
        self._landing_dir = landing_dir
        self._rotate_every = rotate_every
        self._counts: dict[str, int] = {}
        self._handles: dict[str, object] = {}

    def emit(self, event: ChangeEvent) -> None:
        count = self._counts.get(event.entity, 0)
        file_index = count // self._rotate_every + 1
        path = self._landing_dir / event.entity / f"events_{file_index:05d}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(event.line + "\n")
        self._counts[event.entity] = count + 1

    def close(self) -> None:  # file handles are not held open
        return None

    def __enter__(self) -> FileSink:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class KafkaSink:
    """Produces events to ``<topic_prefix>.<entity>`` on a Kafka-compatible broker.

    Uses confluent-kafka with delivery callbacks; ``close()`` flushes and raises
    if any delivery failed, so simulator exit codes are trustworthy in demos.
    """

    def __init__(self, bootstrap_servers: str, topic_prefix: str) -> None:
        # Imported lazily so file mode never touches librdkafka.
        from confluent_kafka import Producer  # noqa: PLC0415

        self._topic_prefix = topic_prefix
        self._errors: list[str] = []
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def _on_delivery(self, err: object, _msg: object) -> None:
        if err is not None:
            self._errors.append(str(err))

    def emit(self, event: ChangeEvent) -> None:
        self._producer.produce(
            topic=f"{self._topic_prefix}.{event.entity}",
            key=event.key.encode("utf-8"),
            value=event.line.encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def close(self) -> None:
        self._producer.flush(30)
        if self._errors:
            raise RuntimeError(f"{len(self._errors)} events failed delivery: {self._errors[:3]}")
