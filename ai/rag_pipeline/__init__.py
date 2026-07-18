"""Retrieval-augmented generation over synthetic banking governance docs.

Fully offline by design: corpus generation is seeded, chunking is
deterministic, and the default embedder is a dependency-free feature-hashing
model. ``sentence-transformers`` upgrades embedding quality when installed
(``uv sync --extra rag-st``) behind the same :class:`Embedder` protocol —
the pipeline code does not change.
"""
