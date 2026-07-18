"""Heading-aware markdown chunking.

Governance documents have meaningful section structure, so the chunker
splits on ``##`` headings first — a chunk is a section with its document
title and heading path attached (that context is what makes citations
useful). Oversized sections are re-split on paragraph boundaries with
overlap so no chunk exceeds the size budget.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

HEADING = re.compile(r"^(#{1,3})\s+(.*)$", re.MULTILINE)


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_slug: str
    doc_title: str
    heading: str
    text: str

    @property
    def citation(self) -> str:
        return f"{self.doc_title} > {self.heading}"


def _split_long(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    """Split on paragraph boundaries, carrying overlap for context continuity."""
    if len(text) <= max_chars:
        return [text]
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    pieces: list[str] = []
    current = ""
    for paragraph in paragraphs:
        if current and len(current) + len(paragraph) + 2 > max_chars:
            pieces.append(current)
            current = current[-overlap_chars:] + "\n\n" if overlap_chars else ""
        current = f"{current}{paragraph}\n\n"
    if current.strip():
        pieces.append(current)
    return [p.strip() for p in pieces]


def chunk_document(path: Path, max_chars: int = 1200, overlap_chars: int = 150) -> list[Chunk]:
    """Chunk one markdown document by heading, then by size."""
    text = path.read_text(encoding="utf-8")
    slug = path.stem

    matches = list(HEADING.finditer(text))
    doc_title = next((m.group(2).strip() for m in matches if len(m.group(1)) == 1), slug)

    chunks: list[Chunk] = []
    sections: list[tuple[str, str]] = []
    for i, match in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[match.end() : end].strip()
        if body:
            sections.append((match.group(2).strip(), body))

    for heading, body in sections:
        for j, piece in enumerate(_split_long(body, max_chars, overlap_chars)):
            suffix = f"-{j}" if j else ""
            chunks.append(
                Chunk(
                    chunk_id=f"{slug}/{_slugify(heading)}{suffix}",
                    doc_slug=slug,
                    doc_title=doc_title,
                    heading=heading,
                    text=piece,
                )
            )
    return chunks


def chunk_corpus(corpus_dir: Path, max_chars: int = 1200, overlap_chars: int = 150) -> list[Chunk]:
    chunks: list[Chunk] = []
    for path in sorted(corpus_dir.glob("*.md")):
        chunks.extend(chunk_document(path, max_chars, overlap_chars))
    if not chunks:
        raise FileNotFoundError(f"no markdown documents under {corpus_dir}")
    return chunks


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
