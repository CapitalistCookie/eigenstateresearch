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
        """Embed texts using Voyage Finance 2 API."""
        if not self.voyage_api_key:
            raise ValueError("VOYAGE_API_KEY is required for embedding")

        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            start = time.time()
            resp = self._http.post(
                VOYAGE_API_URL,
                json={"model": VOYAGE_MODEL, "input": batch, "input_type": "document"},
                headers={"Authorization": f"Bearer {self.voyage_api_key}"},
            )
            resp.raise_for_status()
            data = resp.json()
            batch_embeddings = [item["embedding"] for item in data["data"]]
            all_embeddings.extend(batch_embeddings)
            duration = (time.time() - start) * 1000
            logger.info(
                f"Voyage batch {i // batch_size + 1}: {len(batch)} texts, {duration:.0f}ms"
            )
        return all_embeddings

    def embed_query(self, query: str) -> list[float]:
        """Embed a single search query with input_type=query."""
        if not self.voyage_api_key:
            raise ValueError("VOYAGE_API_KEY is required for embedding")

        resp = self._http.post(
            VOYAGE_API_URL,
            json={"model": VOYAGE_MODEL, "input": [query], "input_type": "query"},
            headers={"Authorization": f"Bearer {self.voyage_api_key}"},
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]

    def close(self):
        self._http.close()
