"""Versioned data contracts.

A contract is the producer/consumer agreement for one entity: fields, types,
nullability, enums, PII classification, and freshness SLA — written as YAML
(reviewable, diffable, versioned) and validated by pydantic models.

The contract is the **single source of truth** consumed by three places:

* silver enforcement (``pipelines.silver``): schema projection + violation
  routing to quarantine,
* the DQ framework (``quality.expectations``): baseline suite generation,
* governance (``governance.unity_catalog``): PII tags are *derived* from the
  contract's ``pii`` flags, never maintained by hand.
"""

from quality.contracts.loader import load_contract, load_contracts
from quality.contracts.models import Contract, FieldSpec

__all__ = ["Contract", "FieldSpec", "load_contract", "load_contracts"]
