"""Batch file-drop simulation: reference data landed as CSV/JSON/Parquet.

Models the other half of real ingestion: not everything arrives as a change
stream. Reference datasets (branches, products, FX rates) show up as periodic
file drops from upstream teams, in whatever format those teams happen to
export. The bronze COPY INTO-style loader (``pipelines.bronze.copy_into``)
consumes this landing zone with an exactly-once file ledger.
"""
