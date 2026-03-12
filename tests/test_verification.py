from __future__ import annotations

from datetime import datetime

import pytest

from ashby_discovery.models import FetchedPage
from ashby_discovery.verification import verify_slug


class FakeFetcher:
    def __init__(self, page: FetchedPage | Exception) -> None:
        self.page = page
        self.last_url: str | None = None

    async def fetch(self, url: str, *, use_cache: bool = True) -> FetchedPage:  # noqa: ARG002
        self.last_url = url
        if isinstance(self.page, Exception):
            raise self.page
        return self.page


@pytest.mark.asyncio
async def test_verify_slug_verified() -> None:
    html = """
    <html>
      <head><title>Acme - Jobs</title></head>
      <body>
        <a href=\"https://jobs.ashbyhq.com/acme/job/123\">Role</a>
        <script>window.Ashby = {};</script>
      </body>
    </html>
    """
    page = FetchedPage(
        url="https://jobs.ashbyhq.com/acme",
        final_url="https://jobs.ashbyhq.com/acme",
        status_code=200,
        text=html,
        fetched_at=datetime.utcnow(),
    )
    result = await verify_slug(FakeFetcher(page), "acme")
    assert result.verification_status == "VERIFIED"
    assert result.inferred_company_name == "Acme"


@pytest.mark.asyncio
async def test_verify_slug_not_verified_for_404() -> None:
    page = FetchedPage(
        url="https://jobs.ashbyhq.com/ghost",
        final_url="https://jobs.ashbyhq.com/ghost",
        status_code=404,
        text="Not Found",
        fetched_at=datetime.utcnow(),
    )
    result = await verify_slug(FakeFetcher(page), "ghost")
    assert result.verification_status == "NOT_VERIFIED"
    assert result.http_status == 404


@pytest.mark.asyncio
async def test_verify_slug_error_on_exception() -> None:
    result = await verify_slug(FakeFetcher(RuntimeError("boom")), "oops")
    assert result.verification_status == "ERROR"


@pytest.mark.asyncio
async def test_verify_slug_encodes_spaces() -> None:
    page = FetchedPage(
        url="https://jobs.ashbyhq.com/Checkbox%20Technology",
        final_url="https://jobs.ashbyhq.com/Checkbox%20Technology",
        status_code=404,
        text="Not Found",
        fetched_at=datetime.utcnow(),
    )
    fetcher = FakeFetcher(page)
    await verify_slug(fetcher, "Checkbox Technology")
    assert fetcher.last_url == "https://jobs.ashbyhq.com/Checkbox%20Technology"
