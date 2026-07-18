"""Shared fixtures: one local SparkSession per test session, tmp lakehouse configs."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipelines.common.config import (
    EntityConfig,
    KafkaConfig,
    LakehouseConfig,
    SourceConfig,
    SparkConfig,
    StorageConfig,
)
from pipelines.common.session import get_spark

DEFAULT_ENTITIES = [
    EntityConfig(name="customers", primary_key="customer_id", scd="type2"),
    EntityConfig(name="accounts", primary_key="account_id", scd="type2"),
    EntityConfig(name="transactions", primary_key="transaction_id", scd="append_only"),
    EntityConfig(name="loan_applications", primary_key="application_id", scd="type2"),
]


def make_config(root: Path, mode: str = "file") -> LakehouseConfig:
    """Lakehouse config rooted at a temp directory; small Spark knobs for tests."""
    return LakehouseConfig(
        source=SourceConfig(
            mode=mode,  # type: ignore[arg-type]
            cdc_landing_dir=root / "landing" / "cdc",
            batch_landing_dir=root / "landing" / "batch",
            kafka=KafkaConfig(bootstrap_servers="localhost:19092", topic_prefix="banking.cdc"),
        ),
        storage=StorageConfig(
            lakehouse_root=root / "lakehouse",
            checkpoints_dir=root / "checkpoints",
            exports_dir=root / "exports",
            warehouse_db=root / "warehouse.duckdb",
            run_dir=root / "run",
            reports_dir=root / "reports",
        ),
        entities=DEFAULT_ENTITIES,
        spark=SparkConfig(driver_memory="2g", shuffle_partitions=2),
    )


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """One Delta-enabled local SparkSession shared by all spark-marked tests."""
    config = make_config(tmp_path_factory.mktemp("spark-session"))
    session = get_spark("lakehouse-tests", config)
    yield session
    session.stop()


@pytest.fixture()
def lakehouse_config(tmp_path: Path) -> LakehouseConfig:
    return make_config(tmp_path)
