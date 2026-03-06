"""Scrape papers from arxiv using the official API."""

import logging
from datetime import datetime, timedelta

import arxiv

from base import BaseScraper
from config import settings
from shared.models import RawDocument

logger = logging.getLogger(__name__)


class ArxivScraper(BaseScraper):
    def __init__(self, redis_url: str):
        super().__init__(redis_url, "arxiv")
        self._rate_limit_delay = 3.0  # arxiv requests 3s between API calls

    def run(self, max_results: int = 200, days_back: int = 7) -> int:
        """Fetch recent papers from arxiv. Returns count of new documents."""
        count = 0

        for category in settings.arxiv_categories:
            try:
                new = self._scrape_category(category, max_results, days_back)
                count += new
                logger.info(f"arxiv/{category}: {new} new papers")
                self.rate_limit()
            except Exception as e:
                logger.error(f"Error scraping arxiv/{category}: {e}")

        logger.info(f"arxiv total: {count} new papers")
        return count

    def _scrape_category(
        self, category: str, max_results: int, days_back: int
    ) -> int:
        """Scrape a single arxiv category."""
        search = arxiv.Search(
            query=f"cat:{category}",
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        cutoff = datetime.now() - timedelta(days=days_back)
        count = 0

        client = arxiv.Client(page_size=50, delay_seconds=3.0, num_retries=3)
        for result in client.results(search):
            if result.published.replace(tzinfo=None) < cutoff:
                break

            # result.categories is a list of strings (e.g. ["q-fin.TR", "stat.ML"])
            categories = result.categories if hasattr(result, "categories") else [category]

            doc = RawDocument(
                source="arxiv",
                url=result.entry_id,
                title=result.title.replace("\n", " ").strip(),
                authors=[a.name for a in result.authors],
                abstract=result.summary.replace("\n", " ").strip(),
                published_date=result.published.strftime("%Y-%m-%d"),
                pdf_url=result.pdf_url,
                tags=categories,
                metadata={
                    "arxiv_id": result.get_short_id(),
                    "categories": categories,
                    "primary_category": result.primary_category,
                },
            )

            if self.submit(doc):
                count += 1

        return count

    def backfill(self, max_results: int = 5000) -> int:
        """Backfill historical papers (larger fetch)."""
        logger.info(f"Starting arxiv backfill (max {max_results} per category)...")
        return self.run(max_results=max_results, days_back=365 * 5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = ArxivScraper(redis_url=settings.redis_url)
    count = scraper.run()
    print(f"Scraped {count} new papers from arxiv")
