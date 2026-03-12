from __future__ import annotations

from datetime import datetime

import pytest

from ashby_discovery.discovery import _search_provider, normalize_search_engines
from ashby_discovery.models import FetchedPage
from ashby_discovery.search_providers import search_urls_for


def test_normalize_search_engines_defaults() -> None:
    assert normalize_search_engines(None) == ["duckduckgo", "brave", "yahoo"]


def test_normalize_search_engines_filters_unsupported() -> None:
    engines = normalize_search_engines(["google", "ddg", "yahoo", "brave", "brave"])
    assert engines == ["duckduckgo", "yahoo", "brave"]


def test_duckduckgo_search_urls_include_html_and_lite() -> None:
    urls = search_urls_for("duckduckgo", "site:jobs.ashbyhq.com", page_idx=1)
    assert urls == [
        "https://html.duckduckgo.com/html/?q=site%3Ajobs.ashbyhq.com&s=30",
        "https://lite.duckduckgo.com/lite/?q=site%3Ajobs.ashbyhq.com&s=30",
    ]


class _FakeFetcher:
    def __init__(self, html: str, status_code: int = 200) -> None:
        self.html = html
        self.status_code = status_code
        self.calls: list[str] = []

    async def fetch(self, url: str, *, use_cache: bool = True) -> FetchedPage:  # noqa: ARG002
        self.calls.append(url)
        return FetchedPage(
            url=url,
            final_url=url,
            status_code=self.status_code,
            text=self.html,
            fetched_at=datetime.utcnow(),
        )


@pytest.mark.asyncio
async def test_search_provider_success_path_executes_without_callable_shadowing() -> None:
    html = """
    <html>
      <body>
        <a class="result__a" href="https://jobs.ashbyhq.com/acme/job/123">Acme role</a>
      </body>
    </html>
    """
    fetcher = _FakeFetcher(html)

    result = await _search_provider(
        fetcher,
        "site:jobs.ashbyhq.com",
        engine="duckduckgo",
        pages=1,
        store=None,
        verbose=False,
        progress=None,
    )

    assert result.blocked_reason is None
    assert any(url.startswith("https://jobs.ashbyhq.com/acme") for url in result.urls)
    assert len(fetcher.calls) >= 1
