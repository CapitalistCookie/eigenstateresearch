"""Main scheduler for all scrapers. Runs daily + continuous file watching."""

import logging
import time

import schedule

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


def run_daily_scrape():
    """Run all scrapers."""
    start = time.time()
    total = 0

    logger.info("=== Starting daily scrape ===")

    try:
        arxiv_scraper = ArxivScraper(settings.redis_url)
        count = arxiv_scraper.run()
        total += count
        logger.info(f"arxiv: {count} new papers")
    except Exception as e:
        logger.error(f"arxiv scraper failed: {e}")

    try:
        ssrn = SSRNScraper(settings.redis_url)
        count = ssrn.run()
        total += count
        logger.info(f"SSRN: {count} new papers")
    except Exception as e:
        logger.error(f"SSRN scraper failed: {e}")

    try:
        blogs = BlogScraper(settings.redis_url)
        count = blogs.run()
        total += count
        logger.info(f"Blogs/RSS: {count} new posts")
    except Exception as e:
        logger.error(f"Blog scraper failed: {e}")

    duration = time.time() - start
    logger.info(f"=== Daily scrape complete: {total} new docs in {duration:.0f}s ===")


def run_internal_scan():
    """Scan internal docs."""
    try:
        watcher = InternalWatcher(settings.redis_url)
        count = watcher.run()
        logger.info(f"Internal scan: {count} new docs")
    except Exception as e:
        logger.error(f"Internal scan failed: {e}")


def main():
    logger.info("Starting research pipeline scheduler...")

    # Initial scan
    run_internal_scan()

    # Schedule daily scrapes at 2:00 AM ET (7:00 UTC)
    schedule.every().day.at("07:00").do(run_daily_scrape)

    # Schedule internal scan every hour (pulls latest git, scans for new docs)
    schedule.every().hour.do(run_internal_scan)

    logger.info("Scheduler ready. Next daily scrape at 07:00 UTC")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
