# Batch Landing

Simulates the file-drop half of real ingestion: reference datasets landed
periodically by upstream teams, in whatever format those teams export.

```bash
uv run python -m ingestion.batch_landing --seed 42
```

| Dataset | Format | Quirk it exercises |
|---|---|---|
| `branches` | CSV | A byte-identical **resend** under a different filename — the file ledger loads both (new path = new file), silver dedup collapses the rows |
| `products` | JSON | Nested-ish app-team export |
| `fx_rates` | Parquet | The well-behaved upstream |

## Why it exists

Streaming CDC alone doesn't represent a real lakehouse's intake. Most
regulated-environment platforms have a long tail of batch feeds with
exactly-once expectations. This landing zone feeds
`pipelines/bronze/copy_into.py`, which implements COPY INTO semantics (load
each file exactly once, tracked in a Delta ledger) — see
`docs/adr/001-copy-into-vs-auto-loader.md` for why that model was chosen
over an Auto Loader-style approach for these feeds.
