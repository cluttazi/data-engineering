"""Query the index: cosine top-k with citations.

The "generation" half of RAG is deliberately extractive here (top chunks
with citations and scores) — zero-paid-infrastructure is a hard requirement
of this repo, and a hallucination-free extractive answer over a governance
corpus is more defensible than a small local LLM's paraphrase. The
retrieved-context format is exactly what an LLM answerer would consume, so
swapping one in is additive (see the incident agent's gated Claude backend
for the pattern).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from ai.rag_pipeline.chunker import Chunk
from ai.rag_pipeline.embeddings import Embedder
from ai.rag_pipeline.store import Index


@dataclass(frozen=True)
class Hit:
    chunk: Chunk
    score: float


def search(index: Index, embedder: Embedder, question: str, top_k: int = 3) -> list[Hit]:
    """Top-k chunks by cosine similarity (vectors are L2-normalized)."""
    if embedder.fingerprint != index.fingerprint:
        raise RuntimeError(
            f"index was built with {index.fingerprint!r} but querying with "
            f"{embedder.fingerprint!r} — rebuild the index (make rag)"
        )
    query_vec = embedder.embed([question])[0]
    scores = index.vectors @ query_vec
    order = np.argsort(scores)[::-1][:top_k]
    return [Hit(chunk=index.chunks[i], score=float(scores[i])) for i in order]


def format_answer(question: str, hits: list[Hit]) -> str:
    lines = [f"Q: {question}", ""]
    if not hits:
        return "\n".join([*lines, "No relevant passages found."])
    best = hits[0]
    lines += [
        f"A (top passage, score {best.score:.3f}) [{best.chunk.citation}]:",
        "",
        _indent(best.chunk.text),
        "",
        "Other sources:",
    ]
    lines += [f"  - {h.chunk.citation} (score {h.score:.3f})" for h in hits[1:]]
    return "\n".join(lines)


def _indent(text: str, prefix: str = "    ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())
