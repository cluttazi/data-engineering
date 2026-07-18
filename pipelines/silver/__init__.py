"""Silver layer: contract-enforced, deduplicated, SCD2-historized entities.

Silver is where raw Debezium payloads become trustworthy tables: payloads are
parsed against the versioned data contracts (``quality/contracts``), violations
are routed to quarantine with reasons, exact duplicates are dropped, and
change history is materialized as SCD Type 2 via Delta ``MERGE INTO``.
"""
