"""Base scraper with shared utilities -- dedup, Redis push, rate limiting."""

import logging
import time

import redis as redis_lib

from shared.models import RawDocument
from shared.queue import push_document

logger = logging.getLogger(__name__)


class BaseScraper:
    def __init__(self, redis_url: str, source_name: str):
        self.redis = redis_lib.from_url(redis_url)
        self.source_name = source_name
        self._seen_keys: set[str] = set()
        self._rate_limit_delay = 1.0  # seconds between requests

    def submit(self, doc: RawDocument) -> bool:
        """Submit a document to the processing queue. Returns True if new."""
        key = doc.dedup_key()
        if key in self._seen_keys:
            return False

        # Check Redis set for previously processed docs
        if self.redis.sismember("research_pipeline:processed_docs", key):
            self._seen_keys.add(key)
            return False

        push_document(self.redis, doc)
        self.redis.sadd("research_pipeline:processed_docs", key)
        self._seen_keys.add(key)
        return True

    def rate_limit(self):
        """Sleep to respect rate limits."""
        time.sleep(self._rate_limit_delay)

    def run(self) -> int:
        """Override in subclass. Returns count of new documents submitted."""
        raise NotImplementedError
