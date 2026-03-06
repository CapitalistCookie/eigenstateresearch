"""Scrape papers from SSRN via search and RSS feeds."""

import logging
import re

import httpx
from bs4 import BeautifulSoup

from base import BaseScraper
from config import settings
from shared.models import RawDocument

logger = logging.getLogger(__name__)

SSRN_SEARCH_URL = "https://papers.ssrn.com/sol3/results.cfm"


class SSRNScraper(BaseScraper):
    def __init__(self, redis_url: str):
        super().__init__(redis_url, "ssrn")
        self._rate_limit_delay = 5.0  # Be polite to SSRN
        self._http = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "ResearchBot/1.0 (academic research aggregator)"},
        )

    def run(self, max_results: int = 100) -> int:
        """Fetch recent papers from SSRN keyword search."""
        count = 0

        search_terms = [
            "algorithmic trading futures",
            "market microstructure order flow",
            "commodity futures trading strategy",
            "machine learning financial markets",
            "mean reversion futures",
            "momentum trading strategy",
            "gold silver copper futures",
            "energy futures trading",
            "agricultural commodity futures",
            "cotton futures market",
        ]

        for term in search_terms:
            try:
                new = self._search(term, max_results=max_results // len(search_terms))
                count += new
                self.rate_limit()
            except Exception as e:
                logger.error(f"SSRN search '{term}' failed: {e}")

        logger.info(f"SSRN total: {count} new papers")
        return count

    def _search(self, query: str, max_results: int = 20) -> int:
        """Search SSRN for papers matching query."""
        resp = self._http.get(
            SSRN_SEARCH_URL,
            params={
                "txtKey_Words": query,
                "sort": "date",
                "cnt": str(min(max_results, 50)),
            },
        )
        if resp.status_code != 200:
            logger.warning(f"SSRN search returned {resp.status_code}")
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")
        count = 0

        for item in soup.select(".result-item, .paper-result"):
            try:
                title_el = item.select_one("a.title, h3 a, .paper-title a")
                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                url = title_el.get("href", "")
                if not url.startswith("http"):
                    url = f"https://papers.ssrn.com{url}"

                # Extract paper ID
                paper_id_match = re.search(r"abstract[_=](\d+)", url)
                paper_id = paper_id_match.group(1) if paper_id_match else ""

                abstract_el = item.select_one(".abstract-text, .description")
                abstract = abstract_el.get_text(strip=True) if abstract_el else ""

                authors_el = item.select(".author-name, .authors a")
                authors = [a.get_text(strip=True) for a in authors_el] or ["Unknown"]

                date_el = item.select_one(".date, .posted-date")
                date_str = date_el.get_text(strip=True) if date_el else "2024-01-01"

                pdf_url = None
                if paper_id:
                    pdf_url = f"https://papers.ssrn.com/sol3/Delivery.cfm?abstractid={paper_id}"

                doc = RawDocument(
                    source="ssrn",
                    url=url,
                    title=title,
                    authors=authors,
                    abstract=abstract,
                    published_date=date_str,
                    pdf_url=pdf_url,
                    tags=["ssrn"],
                    metadata={"ssrn_id": paper_id, "search_query": query},
                )

                if self.submit(doc):
                    count += 1
            except Exception as e:
                logger.warning(f"Failed to parse SSRN result: {e}")

        return count
