"""Main processor service: consumes documents from Redis, processes, indexes."""

import logging
import os
import time

import httpx
import redis

from config import settings
from shared.models import RawDocument, ProcessedChunk
from shared.queue import pop_document, push_document, QUEUE_KEY
from pdf_parser import parse_pdf_bytes, parse_html
from chunker import semantic_chunk
from metadata_extractor import MetadataExtractor
from embedder_client import EmbedderClient
from qdrant_indexer import QdrantIndexer
from trilium_sync import TriliumSync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def process_document(
    doc: RawDocument,
    embedder: EmbedderClient,
    indexer: QdrantIndexer,
    extractor: MetadataExtractor,
    trilium: TriliumSync | None,
    http: httpx.Client,
) -> int:
    """Process a single document end-to-end. Returns number of chunks indexed."""
    start = time.time()
    doc_id = doc.dedup_key()

    # 1. Dedup check
    if indexer.document_exists(doc_id):
        logger.info(f"Skipping duplicate: {doc.title[:60]}")
        return 0

    # 2. Get document text
    text = ""
    if doc.html_content:
        text = parse_html(doc.html_content)
    elif doc.pdf_url:
        try:
            resp = http.get(doc.pdf_url, follow_redirects=True)
            resp.raise_for_status()
            pdf_path = os.path.join(settings.pdf_cache_dir, f"{doc_id}.pdf")
            os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
            with open(pdf_path, "wb") as f:
                f.write(resp.content)
            try:
                text = parse_pdf_bytes(resp.content)
            except Exception as parse_err:
                logger.warning(f"Failed to parse PDF for '{doc.title[:50]}': {parse_err}")
                text = doc.abstract
        except Exception as e:
            logger.error(f"Failed to download PDF for '{doc.title[:50]}': {e}")
            text = doc.abstract  # Fall back to abstract only
    else:
        text = doc.abstract

    if not text.strip():
        logger.warning(f"No text extracted for: {doc.title[:60]}")
        return 0

    # 3. Chunk
    chunks_text = semantic_chunk(text)
    logger.info(f"Chunked into {len(chunks_text)} segments")

    # 4. Extract metadata
    metadata = extractor.extract(doc.title, doc.abstract or text[:2000])

    # 5. Build ProcessedChunks
    source_tag = "internal_research" if doc.source == "internal" else "external"
    processed = [
        ProcessedChunk(
            chunk_text=ct,
            chunk_index=i,
            document_id=doc_id,
            title=doc.title,
            authors=doc.authors,
            source=doc.source,
            source_tag=source_tag,
            published_date=doc.published_date,
            url=doc.url,
            concepts=metadata["concepts"],
            instruments=metadata["instruments"],
            methodology=metadata["methodology"],
            relevance_score=metadata["relevance_score"],
        )
        for i, ct in enumerate(chunks_text)
    ]

    # 6. Embed
    embeddings = embedder.embed([c.chunk_text for c in processed])

    # 7. Index in Qdrant
    count = indexer.index_chunks(processed, embeddings)

    # 8. Sync to Trilium
    if trilium:
        try:
            trilium.create_paper_note(
                title=doc.title,
                source=doc.source,
                url=doc.url,
                authors=doc.authors,
                abstract=doc.abstract or text[:1000],
                published_date=doc.published_date,
                concepts=metadata["concepts"],
                instruments=metadata["instruments"],
                methodology=metadata["methodology"],
                relevance_score=metadata["relevance_score"],
            )
        except Exception as e:
            logger.error(f"Trilium sync failed for '{doc.title[:50]}': {e}")

    duration = time.time() - start
    logger.info(
        f"Processed '{doc.title[:50]}' -> {count} chunks in {duration:.1f}s "
        f"(concepts={metadata['concepts']}, instruments={metadata['instruments']})"
    )
    return count


def main():
    logger.info("Starting research pipeline processor...")
    logger.info(f"Redis: {settings.redis_url}")
    logger.info(f"Qdrant: {settings.qdrant_url}")

    r = redis.from_url(settings.redis_url)
    embedder = EmbedderClient(voyage_api_key=settings.voyage_api_key)
    indexer = QdrantIndexer(
        qdrant_url=settings.qdrant_url,
        collection_name=settings.collection_name,
    )
    extractor = MetadataExtractor(api_key=settings.deepseek_api_key)

    trilium = None
    if settings.trilium_etapi_token:
        trilium = TriliumSync(
            trilium_url=settings.trilium_url,
            etapi_token=settings.trilium_etapi_token,
        )

    http = httpx.Client(timeout=60.0)
    total_processed = 0
    total_chunks = 0

    logger.info("Processor ready. Waiting for documents...")

    doc = None
    while True:
        try:
            doc = pop_document(r, timeout=10)
            if doc is None:
                continue

            chunks = process_document(doc, embedder, indexer, extractor, trilium, http)
            total_processed += 1
            total_chunks += chunks
            doc = None  # Clear after successful processing

            if total_processed % 10 == 0:
                stats = indexer.get_stats()
                logger.info(
                    f"Progress: {total_processed} docs, {total_chunks} chunks. "
                    f"Qdrant: {stats['points_count']} points"
                )
        except KeyboardInterrupt:
            logger.info(f"Shutting down. Processed {total_processed} docs, {total_chunks} chunks")
            break
        except Exception as e:
            logger.error(f"Error processing document: {e}", exc_info=True)
            # Re-queue failed document so it's not lost
            if doc is not None:
                push_document(r, doc)
                logger.info(f"Re-queued failed document: {doc.title[:50]}")
                doc = None
            time.sleep(5)

    embedder.close()
    extractor.close()
    if trilium:
        trilium.close()
    http.close()


if __name__ == "__main__":
    main()
