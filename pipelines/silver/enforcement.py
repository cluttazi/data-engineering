"""Contract enforcement: parse raw payloads, route violations to quarantine.

The split is deliberate two-stage:

1. **Parsing is permissive** — the payload JSON is parsed against the
   contract-compiled ``StructType``; a value Spark cannot coerce becomes
   null. No global lenient casts, no ANSI opt-out.
2. **Enforcement is explicit** — nullability and enum rules from the contract
   turn those nulls (and out-of-domain values) into named violations. A row
   with violations goes to quarantine *with its reasons*; everything else
   proceeds with real types.

This is what "data contracts" means operationally: the YAML that producers
review is byte-for-byte the schema and rule set the pipeline executes.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from quality.contracts.models import Contract
from quality.contracts.spark_schema import to_struct_type


@dataclass(frozen=True)
class EnforcementResult:
    valid: DataFrame
    quarantined: DataFrame
    checked_rules: int


def _violation_checks(contract: Contract, payload_col: str) -> list[Column]:
    """One Column per contract rule, evaluating to a reason string or null."""
    checks: list[Column] = []
    for spec in contract.fields:
        col = F.col(f"{payload_col}.{spec.name}")
        if not spec.nullable:
            checks.append(F.when(col.isNull(), F.lit(f"null_violation:{spec.name}")))
        if spec.allowed_values:
            checks.append(
                F.when(
                    col.isNotNull() & ~col.isin(*spec.allowed_values),
                    F.lit(f"enum_violation:{spec.name}"),
                )
            )
    return checks


def parse_payloads(events: DataFrame, contract: Contract) -> DataFrame:
    """Add typed ``after``/``before`` structs parsed from the raw envelope line.

    Delete events carry ``after = null`` legitimately; enforcement below only
    applies contract rules to ops that carry an after-image.
    """
    schema = to_struct_type(contract)
    return events.withColumn(
        "after", F.from_json(F.get_json_object("raw_value", "$.after"), schema)
    ).withColumn("before", F.from_json(F.get_json_object("raw_value", "$.before"), schema))


def enforce_contract(events: DataFrame, contract: Contract) -> EnforcementResult:
    """Split parsed events into contract-valid rows and quarantined rows."""
    checks = _violation_checks(contract, "after")
    has_after = F.col("op").isin("c", "u", "r")

    with_violations = events.withColumn(
        "violations",
        F.when(
            has_after & F.col("after").isNull(),
            F.array(F.lit("payload_violation:after_missing_or_unparseable")),
        )
        .when(
            has_after,
            F.array_compact(F.array(*checks)) if checks else F.array().cast("array<string>"),
        )
        .otherwise(F.array().cast("array<string>")),
    )

    valid = with_violations.filter(F.size("violations") == 0).drop("violations")
    quarantined = with_violations.filter(F.size("violations") > 0).select(
        F.lit(contract.contract).alias("entity"),
        F.lit(contract.version).cast("int").alias("contract_version"),
        F.col("raw_value").alias("raw_line"),
        F.col("violations"),
        F.col("op"),
        F.col("ts_ms"),
        F.col("run_id"),
        F.current_timestamp().alias("quarantined_at"),
        F.current_date().alias("quarantine_date"),
    )
    return EnforcementResult(valid=valid, quarantined=quarantined, checked_rules=len(checks))
