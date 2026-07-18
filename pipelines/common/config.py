"""Typed loader for ``config/lakehouse.yaml``.

Pydantic gives us validation with useful error messages at process start —
a config typo fails fast instead of surfacing as a cryptic Spark error three
stages later. Environment variables override the two knobs that vary between
local/demo/CI runs (transport mode and storage root).
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, TypeAdapter

SourceMode = Literal["file", "kafka", "auto"]
ScdStrategy = Literal["type2", "append_only"]

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = _REPO_ROOT / "config" / "lakehouse.yaml"


class KafkaConfig(BaseModel):
    bootstrap_servers: str
    topic_prefix: str
    starting_offsets: str = "earliest"


class SourceConfig(BaseModel):
    mode: SourceMode
    cdc_landing_dir: Path
    batch_landing_dir: Path
    kafka: KafkaConfig


class StorageConfig(BaseModel):
    lakehouse_root: Path
    checkpoints_dir: Path
    exports_dir: Path
    warehouse_db: Path
    run_dir: Path
    reports_dir: Path


class EntityConfig(BaseModel):
    name: str
    primary_key: str
    scd: ScdStrategy


class SparkConfig(BaseModel):
    driver_memory: str = "3g"
    shuffle_partitions: int = 8
    session_timezone: str = "UTC"


class LakehouseConfig(BaseModel):
    source: SourceConfig
    storage: StorageConfig
    entities: list[EntityConfig] = Field(min_length=1)
    spark: SparkConfig = SparkConfig()

    def entity(self, name: str) -> EntityConfig:
        for entity in self.entities:
            if entity.name == name:
                return entity
        raise KeyError(f"unknown entity {name!r}; configured: {[e.name for e in self.entities]}")


def _resolve(base: Path, path: Path) -> Path:
    return path if path.is_absolute() else base / path


@lru_cache(maxsize=4)
def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> LakehouseConfig:
    """Load, validate, and env-override the lakehouse configuration.

    All relative paths are anchored at the repo root so every entry point
    (make targets, tests, spark-submit) sees the same absolute layout
    regardless of its working directory.
    """
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config = LakehouseConfig.model_validate(raw)

    if mode := os.environ.get("LAKEHOUSE_SOURCE_MODE"):
        config.source.mode = TypeAdapter(SourceMode).validate_python(mode)

    root = Path(os.environ.get("LAKEHOUSE_ROOT", str(_REPO_ROOT)))
    config.source.cdc_landing_dir = _resolve(root, config.source.cdc_landing_dir)
    config.source.batch_landing_dir = _resolve(root, config.source.batch_landing_dir)
    storage = config.storage
    storage.lakehouse_root = _resolve(root, storage.lakehouse_root)
    storage.checkpoints_dir = _resolve(root, storage.checkpoints_dir)
    storage.exports_dir = _resolve(root, storage.exports_dir)
    storage.warehouse_db = _resolve(root, storage.warehouse_db)
    storage.run_dir = _resolve(root, storage.run_dir)
    storage.reports_dir = _resolve(root, storage.reports_dir)
    return config
