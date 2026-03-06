"""Redis queue helpers for passing RawDocuments between scrapers and processor."""

import logging

import redis as redis_lib

from shared.models import RawDocument

logger = logging.getLogger(__name__)

QUEUE_KEY = "research_pipeline:raw_documents"


def push_document(
    r: redis_lib.Redis, doc: RawDocument, queue_key: str = QUEUE_KEY
) -> int:
    """Push a RawDocument to the Redis queue. Returns new queue length."""
    payload = doc.model_dump_json()
    length = r.rpush(queue_key, payload)
    logger.info(f"Pushed document to queue: {doc.title[:60]} (queue len={length})")
    return length


def pop_document(
    r: redis_lib.Redis, queue_key: str = QUEUE_KEY, timeout: int = 5
) -> RawDocument | None:
    """Pop a RawDocument from the Redis queue. Blocks up to timeout seconds."""
    result = r.blpop(queue_key, timeout=timeout)
    if result is None:
        return None
    _, payload = result
    doc = RawDocument.model_validate_json(payload)
    logger.info(f"Popped document from queue: {doc.title[:60]}")
    return doc
