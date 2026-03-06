"""Show research pipeline statistics."""

import os
import sys

import httpx

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
COLLECTION = "papers"


def main():
    client = httpx.Client(timeout=10.0)

    # Collection stats
    try:
        resp = client.get(f"{QDRANT_URL}/collections/{COLLECTION}")
        info = resp.json()["result"]
        print(f"\n=== Research Pipeline Stats ===")
        print(f"Total vectors: {info['points_count']:,}")
        print(f"Status: {info['status']}")
    except Exception as e:
        print(f"Error connecting to Qdrant at {QDRANT_URL}: {e}")
        return

    # Source breakdown
    print("\nBy source:")
    for source in ["arxiv", "ssrn", "nber", "fed", "blog", "manual", "internal"]:
        resp = client.post(
            f"{QDRANT_URL}/collections/{COLLECTION}/points/count",
            json={"filter": {"must": [{"key": "source", "match": {"value": source}}]}},
        )
        count = resp.json()["result"]["count"]
        if count > 0:
            print(f"  {source:>10}: {count:>6} chunks")

    # Source tag breakdown
    print("\nBy type:")
    for tag in ["external", "internal_research"]:
        resp = client.post(
            f"{QDRANT_URL}/collections/{COLLECTION}/points/count",
            json={"filter": {"must": [{"key": "source_tag", "match": {"value": tag}}]}},
        )
        count = resp.json()["result"]["count"]
        print(f"  {tag:>20}: {count:>6} chunks")

    # Redis queue depth
    try:
        import redis
        redis_url = os.environ.get("REDIS_URL", "redis://10.228.0.3:6379")
        r = redis.from_url(redis_url)
        queue_len = r.llen("research_pipeline:raw_documents")
        processed = r.scard("research_pipeline:processed_docs")
        print(f"\n  Queue depth: {queue_len}")
        print(f"  Total processed: {processed}")
    except Exception:
        pass

    print()


if __name__ == "__main__":
    main()
