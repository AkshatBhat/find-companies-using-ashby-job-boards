from __future__ import annotations

from typing import Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

DEFAULT_SEARCH_ENGINES = ("duckduckgo", "brave", "yahoo")
SUPPORTED_SEARCH_ENGINES = frozenset(DEFAULT_SEARCH_ENGINES)


def search_urls_for(engine: str, query: str, page_idx: int) -> list[str]:
    q = quote_plus(query)
    if engine == "duckduckgo":
        return [
            f"https://html.duckduckgo.com/html/?q={q}&s={page_idx * 30}",
            f"https://lite.duckduckgo.com/lite/?q={q}&s={page_idx * 30}",
        ]
    if engine == "brave":
        return [f"https://search.brave.com/search?q={q}&offset={page_idx * 10}&source=web"]
    if engine == "yahoo":
        return [f"https://search.yahoo.com/search?p={q}&b={page_idx * 10 + 1}"]
    raise ValueError(f"Unsupported search engine: {engine}")


def blocked_reason(engine: str, status_code: int, html: str) -> Optional[str]:
    lower = html.lower()
    if status_code == 429:
        return "rate_limited"
    if status_code == 403:
        return "challenge"

    common_markers = (
        "captcha",
        "verify you are human",
        "unusual traffic",
        "attention required",
        "access denied",
    )
    if any(marker in lower for marker in common_markers):
        return "challenge"

    if engine == "duckduckgo":
        ddg_markers = (
            "anomaly-modal",
            "challenge-form",
            "bots use duckduckgo",
            "please complete the following challenge",
        )
        if status_code == 202 and any(marker in lower for marker in ddg_markers):
            return "challenge"
    return None


def select_result_links(engine: str, soup: BeautifulSoup) -> list[str]:
    selectors = {
        "duckduckgo": "a.result__a, a.result-link, td.result-link a",
        "brave": "a.heading-serpresult, a.snippet-title",
        "yahoo": "div#web h3.title a, h3.title a",
    }
    chosen = selectors.get(engine, "a")
    links = soup.select(chosen)
    if not links:
        links = soup.find_all("a")
    return [link.get("href", "") for link in links if link.get("href")]

