"""Index processed chunks into Qdrant vector database."""

import logging
import uuid
from datetime import datetime, timezone

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Filter,
    FieldCondition,
    MatchValue,
    PointStruct,
    Range,
)

from shared.models import ProcessedChunk

logger = logging.getLogger(__name__)


class QdrantIndexer:
    def __init__(
        self,
        qdrant_url: str = "http://localhost:6333",
        collection_name: str = "papers",
    ):
        self.client = QdrantClient(url=qdrant_url, check_compatibility=False)
        self.collection_name = collection_name

    def index_chunks(
        self, chunks: list[ProcessedChunk], embeddings: list[list[float]]
    ) -> int:
        """Index chunks with their embeddings into Qdrant. Returns count indexed."""
        if len(chunks) != len(embeddings):
            raise ValueError(
                f"Chunks ({len(chunks)}) and embeddings ({len(embeddings)}) must match"
            )

        points = []
        for chunk, embedding in zip(chunks, embeddings):
            point_id = str(uuid.uuid4())
            points.append(
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "chunk_text": chunk.chunk_text,
                        "chunk_index": chunk.chunk_index,
                        "document_id": chunk.document_id,
                        "title": chunk.title,
                        "authors": chunk.authors,
                        "source": chunk.source,
                        "source_tag": chunk.source_tag,
                        "published_date": chunk.published_date,
                        "url": chunk.url,
                        "concepts": chunk.concepts,
                        "instruments": chunk.instruments,
                        "methodology": chunk.methodology,
                        "relevance_score": chunk.relevance_score,
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            )

        # Upsert in batches of 100
        for i in range(0, len(points), 100):
            batch = points[i : i + 100]
            self.client.upsert(collection_name=self.collection_name, points=batch)

        logger.info(
            f"Indexed {len(points)} chunks for '{chunks[0].title[:50]}' "
            f"(doc_id={chunks[0].document_id[:12]})"
        )
        return len(points)

    def document_exists(self, document_id: str) -> bool:
        """Check if a document (by dedup key) already exists in Qdrant."""
        results = self.client.scroll(
            collection_name=self.collection_name,
            scroll_filter=Filter(
                must=[FieldCondition(key="document_id", match=MatchValue(value=document_id))]
            ),
            limit=1,
        )
        points, _ = results
        return len(points) > 0

    def search(
        self,
        query_vector: list[float],
        limit: int = 10,
        source_filter: str | None = None,
        instrument_filter: str | None = None,
        min_relevance: int | None = None,
    ) -> list:
        """Search for similar chunks with optional metadata filters."""
        must_conditions = []
        if source_filter:
            must_conditions.append(
                FieldCondition(key="source", match=MatchValue(value=source_filter))
            )
        if instrument_filter:
            must_conditions.append(
                FieldCondition(key="instruments", match=MatchValue(value=instrument_filter))
            )
        if min_relevance is not None:
            must_conditions.append(
                FieldCondition(key="relevance_score", range=Range(gte=min_relevance))
            )

        search_filter = Filter(must=must_conditions) if must_conditions else None

        results = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
            query_filter=search_filter,
        )
        return results.points

    def get_stats(self) -> dict:
        """Get collection statistics."""
        info = self.client.get_collection(self.collection_name)
        return {
            "points_count": info.points_count,
            "vectors_count": info.vectors_count,
            "status": info.status.value,
        }
