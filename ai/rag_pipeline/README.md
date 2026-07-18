# RAG Pipeline — Offline Retrieval over Banking Governance Docs

Generate a synthetic corpus of internal bank documents → chunk by heading →
embed → query with citations. Fully offline, zero paid infrastructure.

```bash
make rag                                   # build index + run canned queries
uv run python -m ai.rag_pipeline --query "When is the structuring flag raised?"
uv sync --extra rag-st && make rag         # optional: sentence-transformers quality
```

## Design decisions

- **Corpus is generated, never real.** Six governance documents (retention,
  AML monitoring, incident runbook, DQ standard, access control, PII
  handling) written for this repo, with Faker filling incidental metadata
  under a seed. Their content deliberately cross-references the rest of the
  platform — the AML thresholds match the gold job, the DQ severity policy
  matches the expectations framework — so retrieval answers are checkable.
- **Heading-aware chunking.** Governance docs have meaningful structure; a
  chunk is a section carrying its `Document > Heading` path, which is what
  makes citations legible. Oversized sections re-split on paragraph
  boundaries with overlap.
- **`Embedder` protocol with a guaranteed offline backend.** The default is
  deterministic character-n-gram feature hashing (384-dim, L2-normalized,
  stdlib + numpy): no model download, no network, adequate retrieval quality
  at this corpus size — and it keeps `make demo` and CI hermetic.
  `sentence-transformers` (optional extra) slots in behind the same
  protocol; the index stores the embedder fingerprint and refuses
  mismatched queries instead of silently comparing incompatible spaces.
- **Extractive answers, on purpose.** The zero-paid-infra requirement rules
  out API LLMs, and a hallucination-free extract with citation beats a
  small local model's paraphrase for governance content. The retrieved
  context is formatted exactly as an LLM answerer would consume it — see
  the incident agent's gated Claude backend for how the upgrade slots in.
