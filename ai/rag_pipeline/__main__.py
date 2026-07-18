"""CLI: ``python -m ai.rag_pipeline [--build] [--query "..."] [--demo]``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ai.rag_pipeline.chunker import chunk_corpus
from ai.rag_pipeline.corpus import generate_corpus
from ai.rag_pipeline.embeddings import default_embedder
from ai.rag_pipeline.query import format_answer, search
from ai.rag_pipeline.store import build_index, load_index, save_index
from pipelines.common.config import load_config

DEMO_QUERIES = [
    "What is the retention period for transaction records?",
    "When is the structuring flag raised on an account?",
]


def corpus_dir(root: Path) -> Path:
    return root / "corpus"


def index_dir(root: Path) -> Path:
    return root / "rag_index"


def build(root: Path, seed: int) -> None:
    docs = generate_corpus(corpus_dir(root), seed=seed)
    chunks = chunk_corpus(corpus_dir(root))
    embedder = default_embedder()
    index = build_index(chunks, embedder)
    save_index(index, index_dir(root))
    print(
        f"rag: indexed {len(chunks)} chunks from {len(docs)} documents "
        f"(embedder={embedder.fingerprint})"
    )


def answer(root: Path, question: str, top_k: int) -> None:
    index = load_index(index_dir(root))
    embedder = default_embedder()
    hits = search(index, embedder, question, top_k=top_k)
    print(format_answer(question, hits))
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rag_pipeline",
        description="Build and query the offline RAG index over synthetic banking docs.",
    )
    parser.add_argument("--build", action="store_true", help="generate corpus and build the index")
    parser.add_argument("--query", type=str, help="ask a question against the index")
    parser.add_argument("--demo", action="store_true", help="build then run the canned queries")
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    root = load_config().storage.run_dir.parent / "ai"
    if not (args.build or args.query or args.demo):
        parser.error("nothing to do: pass --build, --query, or --demo")

    if args.build or args.demo:
        build(root, seed=args.seed)
    if args.demo:
        for question in DEMO_QUERIES:
            answer(root, question, args.top_k)
    if args.query:
        answer(root, args.query, args.top_k)
    return 0


if __name__ == "__main__":
    sys.exit(main())
