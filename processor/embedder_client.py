"""Embedding client using Voyage Finance 2 API."""

import logging
import time

import httpx

logger = logging.getLogger(__name__)

VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
VOYAGE_MODEL = "voyage-finance-2"


class EmbedderClient:
    def __init__(self, voyage_api_key: str = ""):
        self.voyage_api_key = voyage_api_key
        self._http = httpx.Client(timeout=120.0)

    def embed(self, texts: list[str], batch_size: int = 64) -> list[list[float]]:
        """Embed texts using Voyage Finance 2 API. Retries on rate limit."""
        if not self.voyage_api_key:
            raise ValueError("VOYAGE_API_KEY is required for embedding")

        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            batch_embeddings = self._embed_with_retry(batch, "document")
            all_embeddings.extend(batch_embeddings)
            batch_num = i // batch_size + 1
            total_batches = (len(texts) + batch_size - 1) // batch_size
            logger.info(f"Voyage batch {batch_num}/{total_batches}: {len(batch)} texts")
        return all_embeddings

    def _embed_with_retry(
        self, texts: list[str], input_type: str, max_retries: int = 10
    ) -> list[list[float]]:
        """Embed with exponential backoff on rate limit errors."""
        for attempt in range(max_retries):
            resp = self._http.post(
                VOYAGE_API_URL,
                json={"model": VOYAGE_MODEL, "input": texts, "input_type": input_type},
                headers={"Authorization": f"Bearer {self.voyage_api_key}"},
            )
            if resp.status_code == 429:
                wait = min(2 ** attempt * 5, 120)
                logger.warning(f"Voyage rate limited, waiting {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return [item["embedding"] for item in resp.json()["data"]]
        raise RuntimeError(f"Voyage API rate limited after {max_retries} retries")

    def embed_query(self, query: str) -> list[float]:
        """Embed a single search query with input_type=query."""
        if not self.voyage_api_key:
            raise ValueError("VOYAGE_API_KEY is required for embedding")

        embeddings = self._embed_with_retry([query], "query")
        return embeddings[0]

    def close(self):
        self._http.close()
