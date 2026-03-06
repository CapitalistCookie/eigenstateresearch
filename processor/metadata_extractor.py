"""Extract structured metadata from papers using DeepSeek API."""

import json
import logging
import re

import httpx

logger = logging.getLogger(__name__)

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"

EXTRACTION_PROMPT = """You are a financial research analyst. Extract structured metadata from this academic paper.

Title: {title}

Abstract/Text: {abstract}

Return a JSON object with exactly these fields:
- "concepts": list of 3-8 key concepts (e.g., "order flow", "mean reversion", "volatility clustering", "GARCH", "market microstructure")
- "instruments": list of financial instruments mentioned (use standard symbols: ES, NQ, GC, SI, HG, CL, NG, ZC, ZS, ZW, CT, SPY, QQQ, BTC, ETH, or general terms like "equities", "commodities", "futures", "bonds")
- "methodology": one of "empirical", "theoretical", "ml_based", "survey", "simulation", "mixed"
- "relevance_score": integer 1-5 for relevance to algorithmic futures trading (5=directly applicable, 1=tangentially related)

Return ONLY the JSON object, no other text."""


class MetadataExtractor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._http = httpx.Client(timeout=30.0)

    def extract(self, title: str, abstract: str) -> dict:
        """Extract metadata from title and abstract via DeepSeek."""
        prompt = EXTRACTION_PROMPT.format(
            title=title, abstract=abstract[:2000]  # Truncate long abstracts
        )

        resp = self._http.post(
            DEEPSEEK_URL,
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 500,
            },
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return _parse_response(content)

    def close(self):
        self._http.close()


def _parse_response(text: str) -> dict:
    """Parse DeepSeek response into structured metadata."""
    # Try to extract JSON from response (may be wrapped in markdown code blocks)
    json_match = re.search(r"\{[\s\S]*\}", text)
    if not json_match:
        logger.warning(f"No JSON found in response: {text[:200]}")
        return _default_metadata()

    try:
        data = json.loads(json_match.group())
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON in response: {text[:200]}")
        return _default_metadata()

    return {
        "concepts": data.get("concepts", [])[:8],
        "instruments": data.get("instruments", [])[:10],
        "methodology": data.get("methodology", "unknown"),
        "relevance_score": max(1, min(5, int(data.get("relevance_score", 3)))),
    }


def _default_metadata() -> dict:
    return {
        "concepts": [],
        "instruments": [],
        "methodology": "unknown",
        "relevance_score": 3,
    }
