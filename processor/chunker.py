"""Semantic chunking for research papers. Splits on section boundaries with overlap."""

import re

SECTION_PATTERN = re.compile(
    r"\n(?=(?:Abstract|Introduction|Background|Related Work|Methodology|Methods|"
    r"Data|Results|Discussion|Conclusion|References|Appendix)\s*\n)",
    re.IGNORECASE,
)

# Rough token estimate: 1 token ~ 4 characters
CHARS_PER_TOKEN = 4


def _estimate_tokens(text: str) -> int:
    return len(text) // CHARS_PER_TOKEN


def semantic_chunk(
    text: str, max_tokens: int = 512, overlap_tokens: int = 50
) -> list[str]:
    """Split text into chunks at section boundaries, falling back to paragraph splits."""
    if _estimate_tokens(text) <= max_tokens:
        return [text.strip()] if text.strip() else []

    # First try splitting on section headers
    sections = SECTION_PATTERN.split(text)
    sections = [s.strip() for s in sections if s.strip()]

    chunks = []
    max_chars = max_tokens * CHARS_PER_TOKEN
    overlap_chars = overlap_tokens * CHARS_PER_TOKEN

    current = ""
    for section in sections:
        if _estimate_tokens(current + "\n\n" + section) <= max_tokens:
            current = (current + "\n\n" + section).strip()
        else:
            if current:
                chunks.append(current)
            # If single section is too long, split by paragraphs
            if _estimate_tokens(section) > max_tokens:
                sub_chunks = _split_by_paragraphs(section, max_chars, overlap_chars)
                chunks.extend(sub_chunks)
                current = ""
            else:
                current = section

    if current.strip():
        chunks.append(current.strip())

    # Add overlap between chunks
    if len(chunks) > 1 and overlap_chars > 0:
        chunks = _add_overlap(chunks, overlap_chars)

    return chunks if chunks else [text.strip()]


def _split_by_paragraphs(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    """Split long sections by paragraph boundaries."""
    paragraphs = re.split(r"\n\s*\n", text)
    chunks = []
    current = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if len(current) + len(para) + 2 <= max_chars:
            current = (current + "\n\n" + para).strip()
        else:
            if current:
                chunks.append(current)
            # If single paragraph is still too long, hard split by sentences
            if len(para) > max_chars:
                sentences = re.split(r"(?<=[.!?])\s+", para)
                current = ""
                for sent in sentences:
                    if len(current) + len(sent) + 1 <= max_chars:
                        current = (current + " " + sent).strip()
                    else:
                        if current:
                            chunks.append(current)
                        current = sent
            else:
                current = para

    if current.strip():
        chunks.append(current.strip())
    return chunks


def _add_overlap(chunks: list[str], overlap_chars: int) -> list[str]:
    """Add trailing context from previous chunk to the start of next chunk."""
    result = [chunks[0]]
    for i in range(1, len(chunks)):
        prev_tail = chunks[i - 1][-overlap_chars:]
        # Find a clean word boundary for overlap
        space_idx = prev_tail.find(" ")
        if space_idx > 0:
            prev_tail = prev_tail[space_idx + 1:]
        result.append(f"...{prev_tail}\n\n{chunks[i]}")
    return result
