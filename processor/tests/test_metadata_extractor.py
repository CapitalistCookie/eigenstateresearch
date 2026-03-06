import os

import pytest

from metadata_extractor import MetadataExtractor, _parse_response

DEEPSEEK_KEY = os.environ.get("DEEPSEEK_API_KEY", "")


@pytest.fixture
def extractor():
    return MetadataExtractor(api_key=DEEPSEEK_KEY)


@pytest.mark.skipif(not DEEPSEEK_KEY, reason="No DeepSeek API key")
def test_extract_metadata(extractor):
    title = "Cumulative Order Flow and Market Microstructure in E-mini S&P 500 Futures"
    abstract = (
        "We analyze the predictive power of cumulative order flow (delta) for short-term "
        "price movements in E-mini S&P 500 (ES) futures. Using tick-level data from 2020-2024, "
        "we show that cumulative delta explains 15-20% of forward 5-minute returns, with "
        "significantly higher predictive power during regular trading hours."
    )

    result = extractor.extract(title, abstract)
    assert isinstance(result["concepts"], list)
    assert len(result["concepts"]) > 0
    assert isinstance(result["instruments"], list)
    assert isinstance(result["methodology"], str)
    assert isinstance(result["relevance_score"], int)
    assert 1 <= result["relevance_score"] <= 5


def test_extract_metadata_mock():
    """Test parsing of a mocked DeepSeek response."""
    response_text = """{
        "concepts": ["order flow", "market microstructure", "cumulative delta"],
        "instruments": ["ES"],
        "methodology": "empirical",
        "relevance_score": 5
    }"""
    result = _parse_response(response_text)
    assert result["concepts"] == ["order flow", "market microstructure", "cumulative delta"]
    assert result["instruments"] == ["ES"]
    assert result["methodology"] == "empirical"
    assert result["relevance_score"] == 5
