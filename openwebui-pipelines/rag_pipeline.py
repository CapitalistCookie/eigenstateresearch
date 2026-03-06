"""
OpenWebUI Pipeline: Research RAG
Intercepts queries, retrieves relevant chunks from Qdrant, augments prompt with context.

To install: OpenWebUI Admin -> Pipelines -> Add Pipeline -> paste this file.
"""

import json
import logging
import os
import re
from typing import Optional

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class Pipe:
    class Valves(BaseModel):
        """Configuration exposed in OpenWebUI admin settings."""
        QDRANT_URL: str = Field(default="http://rp-qdrant:6333")
        VOYAGE_API_KEY: str = Field(default="")
        COLLECTION_NAME: str = Field(default="papers")
        TOP_K: int = Field(default=10)
        MIN_SCORE: float = Field(default=0.3)

    def __init__(self):
        self.name = "Research RAG"
        self.valves = self.Valves()

    async def pipe(self, body: dict, __user__: dict = None) -> dict:
        """Main pipeline: retrieve context from Qdrant and augment the prompt."""
        messages = body.get("messages", [])
        if not messages:
            return body

        user_query = messages[-1].get("content", "")
        if not user_query:
            return body

        # Parse any filters from the query
        filters = self._parse_filters(user_query)

        # Embed the query
        query_vector = self._embed_query(user_query)
        if not query_vector:
            return body

        # Search Qdrant
        results = self._search_qdrant(query_vector, filters)
        if not results:
            return body

        # Build context
        context = self._build_context(results)

        # Augment the system message
        system_msg = {
            "role": "system",
            "content": (
                "You are a financial research assistant with access to a knowledge base of "
                "academic papers, research documents, and internal trading research. "
                "Answer questions using the provided context. Always cite sources with "
                "[Title](URL) format. If the context doesn't contain relevant information, "
                "say so clearly.\n\n"
                f"## Retrieved Research Context\n\n{context}"
            ),
        }

        # Prepend system message
        if messages[0].get("role") == "system":
            messages[0] = system_msg
        else:
            messages.insert(0, system_msg)

        body["messages"] = messages
        return body

    def _parse_filters(self, query: str) -> dict:
        """Extract metadata filters from natural language query."""
        filters = {}

        # Source filters
        if re.search(r"\b(ssrn|from ssrn)\b", query, re.I):
            filters["source"] = "ssrn"
        elif re.search(r"\b(arxiv|from arxiv)\b", query, re.I):
            filters["source"] = "arxiv"
        elif re.search(r"\b(our research|internal|our findings)\b", query, re.I):
            filters["source_tag"] = "internal_research"

        # Instrument filters
        instruments = ["ES", "NQ", "GC", "SI", "HG", "CL", "NG", "ZC", "ZS", "ZW", "CT"]
        for inst in instruments:
            if re.search(rf"\b{inst}\b", query):
                filters["instrument"] = inst
                break

        return filters

    def _embed_query(self, query: str) -> Optional[list[float]]:
        """Embed the search query via Voyage Finance 2."""
        try:
            resp = httpx.post(
                "https://api.voyageai.com/v1/embeddings",
                json={
                    "model": "voyage-finance-2",
                    "input": [query],
                    "input_type": "query",
                },
                headers={"Authorization": f"Bearer {self.valves.VOYAGE_API_KEY}"},
                timeout=15.0,
            )
            resp.raise_for_status()
            return resp.json()["data"][0]["embedding"]
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return None

    def _search_qdrant(self, query_vector: list[float], filters: dict) -> list:
        """Search Qdrant with optional filters."""
        must_conditions = []
        for key, value in filters.items():
            if key == "instrument":
                must_conditions.append({
                    "key": "instruments",
                    "match": {"value": value},
                })
            else:
                must_conditions.append({
                    "key": key,
                    "match": {"value": value},
                })

        search_body = {
            "vector": query_vector,
            "limit": self.valves.TOP_K,
            "score_threshold": self.valves.MIN_SCORE,
            "with_payload": True,
        }
        if must_conditions:
            search_body["filter"] = {"must": must_conditions}

        try:
            resp = httpx.post(
                f"{self.valves.QDRANT_URL}/collections/{self.valves.COLLECTION_NAME}/points/search",
                json=search_body,
                timeout=10.0,
            )
            resp.raise_for_status()
            return resp.json().get("result", [])
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}")
            return []

    def _build_context(self, results: list) -> str:
        """Build context string from search results."""
        seen_docs = set()
        context_parts = []

        for r in results:
            payload = r.get("payload", {})
            doc_id = payload.get("document_id", "")

            # Group by document, show max 3 chunks per doc
            if doc_id in seen_docs:
                continue

            title = payload.get("title", "Unknown")
            source = payload.get("source", "")
            source_tag = payload.get("source_tag", "external")
            url = payload.get("url", "")
            chunk_text = payload.get("chunk_text", "")
            score = r.get("score", 0)

            tag_label = "Internal" if source_tag == "internal_research" else source.upper()
            context_parts.append(
                f"### [{title}]({url}) [{tag_label}] (relevance: {score:.2f})\n\n"
                f"{chunk_text[:800]}\n"
            )
            seen_docs.add(doc_id)

        return "\n---\n".join(context_parts) if context_parts else "No relevant documents found."
