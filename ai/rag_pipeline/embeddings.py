"""Embedding backends behind one protocol.

``HashingEmbedder`` is the guaranteed path: deterministic character-n-gram
feature hashing into a fixed-dimension space, L2-normalized — no model
download, no network, no heavyweight dependency, and quality that is
entirely adequate for retrieval over a few dozen governance sections.

``SentenceTransformerEmbedder`` is the quality upgrade
(``uv sync --extra rag-st``), guarded so its absence never breaks the
pipeline. The index records which embedder built it; query-time mismatches
fail loudly instead of silently comparing incompatible spaces.
"""

from __future__ import annotations

import hashlib
import importlib.util
import math
import re
from typing import Protocol

import numpy as np
from numpy.typing import NDArray

Vector = NDArray[np.float32]

TOKEN = re.compile(r"[a-z0-9]+")


class Embedder(Protocol):
    """Text -> L2-normalized vectors."""

    @property
    def fingerprint(self) -> str:
        """Identity string stored in the index; must change if vectors would."""
        ...

    def embed(self, texts: list[str]) -> Vector: ...


class HashingEmbedder:
    """Character n-gram feature hashing with sublinear tf weighting."""

    def __init__(self, dim: int = 384, ngram_range: tuple[int, int] = (3, 5)) -> None:
        self.dim = dim
        self.ngram_range = ngram_range

    @property
    def fingerprint(self) -> str:
        return f"hashing-v1-dim{self.dim}-ngram{self.ngram_range[0]}-{self.ngram_range[1]}"

    def _features(self, text: str) -> dict[int, float]:
        counts: dict[int, float] = {}
        words = TOKEN.findall(text.lower())
        for word in words:
            padded = f"^{word}$"
            lo, hi = self.ngram_range
            for n in range(lo, hi + 1):
                for i in range(max(1, len(padded) - n + 1)):
                    gram = padded[i : i + n]
                    digest = hashlib.blake2b(gram.encode(), digest_size=8).digest()
                    bucket = int.from_bytes(digest, "big") % self.dim
                    counts[bucket] = counts.get(bucket, 0.0) + 1.0
        return counts

    def embed(self, texts: list[str]) -> Vector:
        matrix = np.zeros((len(texts), self.dim), dtype=np.float32)
        for row, text in enumerate(texts):
            for bucket, count in self._features(text).items():
                matrix[row, bucket] = 1.0 + math.log(count)  # sublinear tf
            norm = float(np.linalg.norm(matrix[row]))
            if norm > 0:
                matrix[row] /= norm
        return matrix


class SentenceTransformerEmbedder:
    """all-MiniLM wrapper; import-guarded optional dependency."""

    MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

    def __init__(self) -> None:
        # Lazy: importing torch costs seconds and this backend is optional.
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415

        self._model = SentenceTransformer(self.MODEL_NAME)

    @property
    def fingerprint(self) -> str:
        return f"sentence-transformers/{self.MODEL_NAME.split('/')[-1]}"

    def embed(self, texts: list[str]) -> Vector:
        vectors = self._model.encode(texts, normalize_embeddings=True)
        return np.asarray(vectors, dtype=np.float32)


def default_embedder() -> Embedder:
    """Best available backend: sentence-transformers if installed, else hashing."""
    if importlib.util.find_spec("sentence_transformers") is not None:
        try:
            return SentenceTransformerEmbedder()
        except Exception:  # model download blocked, GPU issues, ...
            pass
    return HashingEmbedder()
