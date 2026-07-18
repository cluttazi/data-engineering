"""RAG pipeline tests: corpus, chunking, embedding, retrieval — no Spark, no network."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from ai.rag_pipeline.chunker import chunk_corpus, chunk_document
from ai.rag_pipeline.corpus import generate_corpus
from ai.rag_pipeline.embeddings import HashingEmbedder
from ai.rag_pipeline.query import search
from ai.rag_pipeline.store import build_index, load_index, save_index


@pytest.fixture(scope="module")
def corpus_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("corpus")
    generate_corpus(out, seed=42)
    return out


def test_corpus_is_deterministic(tmp_path: Path) -> None:
    docs_a = generate_corpus(tmp_path / "a", seed=42)
    docs_b = generate_corpus(tmp_path / "b", seed=42)
    for a, b in zip(docs_a, docs_b, strict=True):
        assert a.path.read_bytes() == b.path.read_bytes()


def test_chunker_is_heading_aware(corpus_dir: Path) -> None:
    chunks = chunk_document(corpus_dir / "data-retention-policy.md")
    headings = {c.heading for c in chunks}
    assert "Retention periods by data class" in headings
    retention = next(c for c in chunks if c.heading == "Retention periods by data class")
    assert retention.doc_title == "Data Retention and Archival Policy"
    assert "10 years" in retention.text
    assert (
        retention.citation == "Data Retention and Archival Policy > Retention periods by data class"
    )


def test_long_sections_split_with_ids(corpus_dir: Path) -> None:
    chunks = chunk_document(corpus_dir / "data-retention-policy.md", max_chars=200)
    default_chunks = chunk_document(corpus_dir / "data-retention-policy.md")
    ids = [c.chunk_id for c in chunks]
    assert len(ids) == len(set(ids)), "chunk ids must stay unique after splitting"
    assert len(chunks) > len(default_chunks), "smaller budget must produce more chunks"
    # content is never lost by splitting (paragraphs are kept whole, so a
    # single long paragraph may exceed the budget — that's by design)
    joined = "\n".join(c.text for c in chunks)
    assert "retained for 10 years" in joined


def test_hashing_embedder_deterministic_and_normalized() -> None:
    embedder = HashingEmbedder()
    a = embedder.embed(["transaction retention period", "unrelated text"])
    b = embedder.embed(["transaction retention period", "unrelated text"])
    assert np.allclose(a, b)
    norms = np.linalg.norm(a, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-5)


def test_hashing_embedder_similarity_ordering() -> None:
    embedder = HashingEmbedder()
    vecs = embedder.embed(
        [
            "retention period for transaction records",
            "transaction records are retained for ten years",
            "streamlit dashboard bar chart rendering",
        ]
    )
    similar = float(vecs[0] @ vecs[1])
    dissimilar = float(vecs[0] @ vecs[2])
    assert similar > dissimilar


def test_index_roundtrip_and_retrieval(corpus_dir: Path, tmp_path: Path) -> None:
    chunks = chunk_corpus(corpus_dir)
    embedder = HashingEmbedder()
    index = build_index(chunks, embedder)
    save_index(index, tmp_path / "index")
    loaded = load_index(tmp_path / "index")
    assert loaded.fingerprint == embedder.fingerprint
    assert len(loaded.chunks) == len(chunks)

    hits = search(loaded, embedder, "What is the retention period for transaction records?")
    assert hits[0].chunk.doc_slug == "data-retention-policy"
    assert "10 years" in hits[0].chunk.text


def test_fingerprint_mismatch_fails_loudly(corpus_dir: Path, tmp_path: Path) -> None:
    chunks = chunk_corpus(corpus_dir)
    index = build_index(chunks, HashingEmbedder(dim=384))
    other = HashingEmbedder(dim=128)
    with pytest.raises(RuntimeError, match="rebuild the index"):
        search(index, other, "anything")
