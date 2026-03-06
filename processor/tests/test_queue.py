"""Tests for Redis queue helpers. Requires Redis running."""

import os

import pytest
import redis

from shared.models import RawDocument
from shared.queue import push_document, pop_document, QUEUE_KEY

REDIS_URL = os.environ.get("REDIS_URL", "redis://10.228.0.3:6379")


@pytest.fixture
def redis_client():
    r = redis.from_url(REDIS_URL)
    # Use a test-specific queue key
    test_key = f"{QUEUE_KEY}:test"
    r.delete(test_key)
    yield r, test_key
    r.delete(test_key)


def test_push_and_pop(redis_client):
    r, key = redis_client
    doc = RawDocument(
        source="arxiv",
        url="https://arxiv.org/abs/test",
        title="Test Paper",
        authors=["Test Author"],
        abstract="Test abstract",
        published_date="2024-01-01",
        tags=["test"],
        metadata={},
    )
    push_document(r, doc, queue_key=key)
    assert r.llen(key) == 1

    popped = pop_document(r, queue_key=key, timeout=1)
    assert popped is not None
    assert popped.title == "Test Paper"
    assert popped.source == "arxiv"
    assert r.llen(key) == 0


def test_pop_empty_returns_none(redis_client):
    r, key = redis_client
    popped = pop_document(r, queue_key=key, timeout=1)
    assert popped is None
