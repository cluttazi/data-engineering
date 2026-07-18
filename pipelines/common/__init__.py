"""Shared pipeline kernel: configuration and Spark session factory.

This package is the one intentional cross-module dependency: ingestion,
quality, observability, and orchestration all read the same
``config/lakehouse.yaml`` through :mod:`pipelines.common.config`, so paths and
entity definitions have exactly one source of truth.
"""
