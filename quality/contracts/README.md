# Data Contracts

Versioned YAML agreements between producers and consumers, validated by
pydantic and compiled to Spark schemas.

```bash
uv run python -m quality.contracts.compat   # verify version lineages
```

## Why contracts, and why here

The failure mode contracts prevent is *silent semantic drift*: a producer
renames a column or loosens an enum, and three teams downstream discover it
in production dashboards. Making the agreement an explicit, versioned,
reviewed artifact moves that discovery to the pull request.

In this repo the contract is **executable**, which is what separates it from
documentation:

- `spark_schema.py` compiles it into the `StructType` silver parses payloads
  against,
- `pipelines/silver/enforcement.py` turns its nullability/enum rules into
  quarantine routing with named violations,
- `governance/unity_catalog/render.py` derives UC PII tags from its `pii`
  flags,
- `quality/expectations` suites cover the same domains at the table level.

One reviewed file, four enforcement surfaces.

## Versioning

Multiple versions coexist on disk (`customers.v1.yaml`, `customers.v2.yaml`);
the highest version is enforced. `compat.py` gates evolution in CI: additive
nullable fields and enum widening are allowed; removals, type changes,
nullability tightening, enum shrinking, and key changes are breaking. The
included v1→v2 example (adds nullable `preferred_language`) flows through the
silver MERGE via Delta schema evolution without manual DDL.
