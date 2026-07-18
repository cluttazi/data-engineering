"""Vector index persistence: numpy matrix + JSON metadata, nothing fancier.

At this corpus size a vector database would be pure ceremony; the honest
engineering choice is a memory-mapped ``.npy`` and a metadata JSON that
records the embedder fingerprint, so a query with a mismatched backend
fails loudly instead of returning garbage similarities.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from ai.rag_pipeline.chunker import Chunk
from ai.rag_pipeline.embeddings import Embedder, Vector


@dataclass(frozen=True)
class Index:
    chunks: list[Chunk]
    vectors: Vector
    fingerprint: str


def build_index(chunks: list[Chunk], embedder: Embedder) -> Index:
    vectors = embedder.embed([f"{c.citation}\n{c.text}" for c in chunks])
    return Index(chunks=chunks, vectors=vectors, fingerprint=embedder.fingerprint)


def save_index(index: Index, index_dir: Path) -> None:
    index_dir.mkdir(parents=True, exist_ok=True)
    np.save(index_dir / "vectors.npy", index.vectors)
    metadata = {
        "fingerprint": index.fingerprint,
        "chunks": [
            {
                "chunk_id": c.chunk_id,
                "doc_slug": c.doc_slug,
                "doc_title": c.doc_title,
                "heading": c.heading,
                "text": c.text,
            }
            for c in index.chunks
        ],
    }
    (index_dir / "index.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
    )


def load_index(index_dir: Path) -> Index:
    metadata = json.loads((index_dir / "index.json").read_text(encoding="utf-8"))
    vectors = np.load(index_dir / "vectors.npy")
    chunks = [Chunk(**c) for c in metadata["chunks"]]
    return Index(
        chunks=chunks, vectors=vectors.astype(np.float32), fingerprint=metadata["fingerprint"]
    )
