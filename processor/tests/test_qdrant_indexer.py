import os

import pytest
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from shared.models import ProcessedChunk
from qdrant_indexer import QdrantIndexer

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
TEST_COLLECTION = "papers_test"


@pytest.fixture
def indexer():
    client = QdrantClient(url=QDRANT_URL, check_compatibility=False)
    # Create test collection
    if client.collection_exists(TEST_COLLECTION):
        client.delete_collection(TEST_COLLECTION)
    client.create_collection(
        TEST_COLLECTION, vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
    )
    idx = QdrantIndexer(qdrant_url=QDRANT_URL, collection_name=TEST_COLLECTION)
    yield idx
    client.delete_collection(TEST_COLLECTION)
    client.close()


def test_index_and_search(indexer):
    chunks = [
        ProcessedChunk(
            chunk_text="Order flow dynamics in ES futures markets show predictive power.",
            chunk_index=0,
            document_id="abc123",
            title="Order Flow Study",
            authors=["Smith, J."],
            source="arxiv",
            source_tag="external",
            published_date="2024-01-15",
            url="https://arxiv.org/abs/test",
            concepts=["order flow", "market microstructure"],
            instruments=["ES"],
            methodology="empirical",
            relevance_score=4,
        )
    ]
    # Fake embedding (1024-dim)
    embeddings = [[0.1] * 1024]

    indexer.index_chunks(chunks, embeddings)

    # Search
    results = indexer.search([0.1] * 1024, limit=1)
    assert len(results) == 1
    assert results[0].payload["title"] == "Order Flow Study"
    assert results[0].payload["source"] == "arxiv"


def test_dedup_check(indexer):
    chunks = [
        ProcessedChunk(
            chunk_text="Test chunk",
            chunk_index=0,
            document_id="dedup_test_id",
            title="Dedup Test",
            authors=["Author"],
            source="ssrn",
            source_tag="external",
            published_date="2024-01-01",
            url="https://ssrn.com/test",
        )
    ]
    embeddings = [[0.2] * 1024]
    indexer.index_chunks(chunks, embeddings)

    assert indexer.document_exists("dedup_test_id") is True
    assert indexer.document_exists("nonexistent") is False
