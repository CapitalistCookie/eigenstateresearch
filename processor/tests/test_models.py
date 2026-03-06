from shared.models import RawDocument


def test_raw_document_serialization():
    doc = RawDocument(
        source="arxiv",
        url="https://arxiv.org/abs/2401.00001",
        title="Order Flow and Market Microstructure",
        authors=["Smith, J.", "Doe, A."],
        abstract="We study order flow dynamics...",
        published_date="2024-01-15",
        pdf_url="https://arxiv.org/pdf/2401.00001",
        tags=["microstructure", "order-flow"],
        metadata={"arxiv_id": "2401.00001", "categories": ["q-fin.TR"]},
    )
    serialized = doc.model_dump_json()
    restored = RawDocument.model_validate_json(serialized)
    assert restored.source == "arxiv"
    assert restored.title == "Order Flow and Market Microstructure"
    assert len(restored.authors) == 2
    assert restored.metadata["arxiv_id"] == "2401.00001"


def test_raw_document_dedup_key():
    doc = RawDocument(
        source="ssrn",
        url="https://ssrn.com/abstract=1234567",
        title="Order Flow and Market Microstructure",
        authors=["Smith, J."],
        abstract="Abstract text",
        published_date="2024-01-15",
        tags=[],
        metadata={},
    )
    key = doc.dedup_key()
    assert isinstance(key, str)
    assert len(key) == 64  # SHA-256 hex digest

    # Same title+author -> same key regardless of source
    doc2 = RawDocument(
        source="arxiv",
        url="https://arxiv.org/abs/9999",
        title="Order Flow and Market Microstructure",
        authors=["Smith, J."],
        abstract="Different abstract",
        published_date="2024-06-01",
        tags=[],
        metadata={},
    )
    assert doc.dedup_key() == doc2.dedup_key()
