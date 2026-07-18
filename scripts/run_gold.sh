#!/usr/bin/env bash
# Run the Scala gold job through the pyspark-shipped spark-submit.
#
# The delta jars are resolved from the local delta-spark wheel (no network),
# by pointing --jars at site-packages/delta/jars when available; otherwise we
# fall back to --packages resolution from Maven Central.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

JAR="$(ls pipelines/gold_scala/target/scala-2.13/gold_2.13-*.jar 2>/dev/null | head -1 || true)"
if [ -z "$JAR" ]; then
    echo "gold jar not found - building with sbt (scripts/sbt package)..." >&2
    # No -batch: with explicit commands sbt is non-interactive anyway, and the
    # flag is rejected when `sbt` is a raw sbt-launch shim (official runner only).
    ./scripts/sbt package >&2
    JAR="$(ls pipelines/gold_scala/target/scala-2.13/gold_2.13-*.jar | head -1)"
fi

RUN_ID="${LAKEHOUSE_RUN_ID:-adhoc-gold}"
LAKEHOUSE_ROOT="${LAKEHOUSE_DATA_ROOT:-$REPO_ROOT/data/lakehouse}"

DELTA_JARS_DIR="$(uv run python - <<'PY'
import pathlib

import delta

jars = pathlib.Path(delta.__file__).parent / "jars"
print(jars if jars.is_dir() else "")
PY
)"

if [ -n "$DELTA_JARS_DIR" ]; then
    DELTA_JARS="$(ls "$DELTA_JARS_DIR"/*.jar | paste -sd, -)"
    DELTA_ARGS=(--jars "$DELTA_JARS")
else
    DELTA_ARGS=(--packages "io.delta:delta-spark_2.13:4.3.1")
fi

exec uv run spark-submit \
    --master "local[2]" \
    --driver-memory 3g \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.ui.enabled=false \
    "${DELTA_ARGS[@]}" \
    --class com.lakehouse.gold.DailyAccountAggregates \
    "$JAR" \
    --lakehouse-root "$LAKEHOUSE_ROOT" \
    --run-id "$RUN_ID"
