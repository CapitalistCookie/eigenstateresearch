import os

import pytest

from embedder_client import EmbedderClient

VOYAGE_KEY = os.environ.get("VOYAGE_API_KEY", "")


@pytest.fixture
def client():
    return EmbedderClient(voyage_api_key=VOYAGE_KEY)


@pytest.mark.skipif(not VOYAGE_KEY, reason="No Voyage API key")
def test_embed_documents(client):
    texts = ["Order flow dynamics in E-mini S&P 500 futures"]
    embeddings = client.embed(texts)
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


@pytest.mark.skipif(not VOYAGE_KEY, reason="No Voyage API key")
def test_embed_query(client):
    vector = client.embed_query("What papers discuss cumulative delta?")
    assert len(vector) == 1024


def test_embed_without_key_raises():
    client = EmbedderClient(voyage_api_key="")
    with pytest.raises(ValueError, match="VOYAGE_API_KEY"):
        client.embed(["test"])
