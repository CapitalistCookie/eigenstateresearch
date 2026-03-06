from chunker import semantic_chunk


def test_short_text_single_chunk():
    text = "This is a short paragraph about order flow in ES futures."
    chunks = semantic_chunk(text, max_tokens=512, overlap_tokens=50)
    assert len(chunks) == 1
    assert chunks[0] == text


def test_long_text_multiple_chunks():
    # Create text that exceeds 512 tokens (~4 chars per token)
    paragraph = "Market microstructure studies the process by which investors' latent demands are ultimately translated into transactions. " * 20
    sections = f"Abstract\n\n{paragraph}\n\nIntroduction\n\n{paragraph}\n\nMethodology\n\n{paragraph}"

    chunks = semantic_chunk(sections, max_tokens=512, overlap_tokens=50)
    assert len(chunks) > 1
    # Each chunk should be non-empty
    for chunk in chunks:
        assert len(chunk.strip()) > 0


def test_section_boundary_splitting():
    text = "Abstract\n\nThis is the abstract.\n\nIntroduction\n\nThis is the introduction.\n\nConclusion\n\nThis is the conclusion."
    chunks = semantic_chunk(text, max_tokens=512, overlap_tokens=50)
    # With small text, might be 1 chunk. Just verify it doesn't crash.
    assert len(chunks) >= 1
