"""Scrape quantitative finance blogs via RSS feeds."""

import logging
from datetime import datetime

import feedparser

from base import BaseScraper
from config import settings
from feeds import BLOG_FEEDS, NBER_FEED, FED_FEEDS
from shared.models import RawDocument

logger = logging.getLogger(__name__)


class BlogScraper(BaseScraper):
    def __init__(self, redis_url: str):
        super().__init__(redis_url, "blog")
        self._rate_limit_delay = 2.0

    def run(self) -> int:
        """Fetch recent posts from all blog RSS feeds."""
        count = 0
        all_feeds = {
            **{f"blog_{k}": v for k, v in BLOG_FEEDS.items()},
            "nber": NBER_FEED,
            **{f"fed_{k}": v for k, v in FED_FEEDS.items()},
        }

        for name, url in all_feeds.items():
            try:
                source = "nber" if "nber" in name else ("fed" if "fed" in name else "blog")
                new = self._scrape_feed(url, source, name)
                count += new
                logger.info(f"{name}: {new} new posts")
            except Exception as e:
                logger.error(f"Feed {name} failed: {e}")
            self.rate_limit()

        logger.info(f"Blog/RSS total: {count} new posts")
        return count

    def _scrape_feed(self, feed_url: str, source: str, feed_name: str) -> int:
        """Parse a single RSS feed."""
        feed = feedparser.parse(feed_url)
        count = 0

        for entry in feed.entries[:50]:  # Cap at 50 per feed
            title = entry.get("title", "").strip()
            if not title:
                continue

            link = entry.get("link", "")
            summary = entry.get("summary", entry.get("description", ""))

            # Parse date
            published = entry.get("published_parsed", entry.get("updated_parsed"))
            if published:
                date_str = datetime(*published[:6]).strftime("%Y-%m-%d")
            else:
                date_str = datetime.now().strftime("%Y-%m-%d")

            authors = []
            if "author" in entry:
                authors = [entry["author"]]
            elif "authors" in entry:
                authors = [a.get("name", "") for a in entry["authors"]]
            if not authors:
                authors = [feed_name]

            doc = RawDocument(
                source=source,
                url=link,
                title=title,
                authors=authors,
                abstract=summary[:2000] if summary else "",
                published_date=date_str,
                html_content=summary,
                tags=[feed_name, source],
                metadata={"feed_name": feed_name, "feed_url": feed_url},
            )

            if self.submit(doc):
                count += 1

        return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = BlogScraper(redis_url=settings.redis_url)
    count = scraper.run()
    print(f"Scraped {count} new posts from blogs/RSS")
