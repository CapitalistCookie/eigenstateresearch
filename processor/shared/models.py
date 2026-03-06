"""Shared data models for the research pipeline."""

import hashlib
import re

from pydantic import BaseModel


class RawDocument(BaseModel):
    """Document as received from a scraper, before processing."""

    source: str  # arxiv, ssrn, nber, fed, eia, usda, blog, manual, internal
    url: str
    title: str
    authors: list[str]
    abstract: str
    published_date: str  # ISO 8601 date string
    pdf_url: str | None = None
    html_content: str | None = None
    tags: list[str] = []
    metadata: dict = {}

    def dedup_key(self) -> str:
        """SHA-256 of normalized title + first author last name."""
        norm_title = re.sub(r"\s+", " ", self.title.lower().strip())
        first_author = self.authors[0].split(",")[0].strip().lower() if self.authors else ""
        raw = f"{norm_title}|{first_author}"
        return hashlib.sha256(raw.encode()).hexdigest()


class ProcessedChunk(BaseModel):
    """A single chunk ready for embedding and indexing."""

    chunk_text: str
    chunk_index: int
    document_id: str  # dedup_key of the source document
    title: str
    authors: list[str]
    source: str
    source_tag: str  # "external" or "internal_research"
    published_date: str
    url: str
    concepts: list[str] = []
    instruments: list[str] = []
    methodology: str = ""
    relevance_score: int = 0
