"""Parse PDFs and HTML into clean text."""

import logging

import pymupdf
from trafilatura import extract

logger = logging.getLogger(__name__)


def parse_pdf_bytes(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pymupdf."""
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    text_parts = []
    for page in doc:
        text_parts.append(page.get_text())
    doc.close()
    return "\n".join(text_parts).strip()


def parse_pdf_file(path: str) -> str:
    """Extract text from a PDF file path."""
    with open(path, "rb") as f:
        return parse_pdf_bytes(f.read())


def parse_html(html: str) -> str:
    """Extract clean text from HTML using trafilatura."""
    import re

    text = extract(html, include_comments=False, include_tables=True)
    if text is None:
        # Fallback: strip tags manually
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()
    return text
