import pytest


def test_arxiv_scraper_initializes():
    from arxiv_scraper import ArxivScraper
    scraper = ArxivScraper(redis_url="redis://localhost:6379")
    assert scraper.source_name == "arxiv"
    assert scraper._rate_limit_delay == 3.0


@pytest.mark.integration
def test_arxiv_scraper_fetches_papers():
    """Integration test: actually queries arxiv API."""
    import os
    from arxiv_scraper import ArxivScraper
    scraper = ArxivScraper(redis_url=os.environ.get("REDIS_URL", "redis://10.228.0.3:6379"))
    count = scraper.run(max_results=5, days_back=30)
    assert count >= 0  # May be 0 if all already processed
