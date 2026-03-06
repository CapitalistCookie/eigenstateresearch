"""CLI tool to manually submit a URL or PDF for processing."""

import argparse
import logging

import httpx

from base import BaseScraper
from config import settings
from shared.models import RawDocument

logger = logging.getLogger(__name__)


def submit_url(url: str, title: str = "", source: str = "manual"):
    """Submit a URL for processing."""
    scraper = BaseScraper(redis_url=settings.redis_url, source_name="manual")

    # Try to fetch title if not provided
    if not title:
        try:
            from bs4 import BeautifulSoup

            resp = httpx.get(url, follow_redirects=True, timeout=15.0)
            soup = BeautifulSoup(resp.text, "html.parser")
            title = soup.title.string.strip() if soup.title else url
        except Exception:
            title = url

    doc = RawDocument(
        source=source,
        url=url,
        title=title,
        authors=["Manual submission"],
        abstract="",
        published_date="",
        pdf_url=url if url.endswith(".pdf") else None,
        html_content=None,
        tags=["manual"],
        metadata={"submitted_via": "cli"},
    )

    if scraper.submit(doc):
        print(f"Submitted: {title}")
    else:
        print(f"Already processed: {title}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Submit a URL for processing")
    parser.add_argument("url", help="URL to submit")
    parser.add_argument("--title", default="", help="Paper title (auto-detected if omitted)")
    parser.add_argument("--source", default="manual", help="Source tag")
    args = parser.parse_args()
    submit_url(args.url, args.title, args.source)
