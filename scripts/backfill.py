"""Run historical backfill across all sources. Progress reporting included."""

import argparse
import logging
import os
import sys
import time

# Add parent directories to path for local execution
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processor"))

from config import settings
from arxiv_scraper import ArxivScraper
from ssrn_scraper import SSRNScraper
from blog_scraper import BlogScraper
from internal_watcher import InternalWatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill research papers")
    parser.add_argument(
        "--sources",
        nargs="+",
        default=["all"],
        choices=["all", "arxiv", "ssrn", "blogs", "internal"],
        help="Which sources to backfill",
    )
    parser.add_argument(
        "--arxiv-max", type=int, default=5000, help="Max papers per arxiv category"
    )
    args = parser.parse_args()

    sources = args.sources if "all" not in args.sources else ["arxiv", "ssrn", "blogs", "internal"]
    total = 0
    start = time.time()

    if "internal" in sources:
        logger.info("=== Backfilling internal research docs ===")
        watcher = InternalWatcher(settings.redis_url)
        count = watcher.run()
        total += count
        logger.info(f"Internal: {count} documents queued")

    if "arxiv" in sources:
        logger.info("=== Backfilling arxiv (this may take a while) ===")
        arxiv_scraper = ArxivScraper(settings.redis_url)
        count = arxiv_scraper.backfill(max_results=args.arxiv_max)
        total += count
        logger.info(f"arxiv: {count} papers queued")

    if "ssrn" in sources:
        logger.info("=== Backfilling SSRN ===")
        ssrn = SSRNScraper(settings.redis_url)
        count = ssrn.run(max_results=500)
        total += count
        logger.info(f"SSRN: {count} papers queued")

    if "blogs" in sources:
        logger.info("=== Backfilling blogs/RSS ===")
        blogs = BlogScraper(settings.redis_url)
        count = blogs.run()
        total += count
        logger.info(f"Blogs: {count} posts queued")

    duration = time.time() - start
    logger.info(f"=== Backfill complete: {total} documents queued in {duration:.0f}s ===")
    logger.info("Documents are now being processed by the processor service.")
    logger.info("Monitor progress: curl http://localhost:6333/collections/papers | python3 -m json.tool")


if __name__ == "__main__":
    main()
