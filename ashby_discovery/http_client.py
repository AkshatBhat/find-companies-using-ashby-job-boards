from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

import httpx

from .models import FetchedPage
from .storage import DiscoveryStore


class AsyncRateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval = max(min_interval_seconds, 0.0)
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        if self.min_interval <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
            self._next_allowed = loop.time() + self.min_interval


class AsyncFetcher:
    def __init__(
        self,
        store: Optional[DiscoveryStore],
        *,
        timeout_seconds: float = 15.0,
        max_connections: int = 20,
        retries: int = 3,
        min_interval_seconds: float = 0.3,
        cache_ttl_seconds: int = 24 * 3600,
        user_agent: str = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    ) -> None:
        self.store = store
        self.retries = max(retries, 1)
        self.cache_ttl_seconds = cache_ttl_seconds
        self.rate_limiter = AsyncRateLimiter(min_interval_seconds)
        self.semaphore = asyncio.Semaphore(max_connections)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections),
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch(self, url: str, *, use_cache: bool = True) -> FetchedPage:
        if self.store and use_cache:
            cached = self.store.load_cached_fetch(url, ttl_seconds=self.cache_ttl_seconds)
            if cached is not None:
                return cached

        last_error: Exception | None = None
        async with self.semaphore:
            for attempt in range(1, self.retries + 1):
                try:
                    await self.rate_limiter.wait()
                    response = await self.client.get(url)
                    page = FetchedPage(
                        url=url,
                        final_url=str(response.url),
                        status_code=response.status_code,
                        text=response.text,
                        fetched_at=datetime.utcnow(),
                    )
                    if self.store and use_cache:
                        self.store.save_cached_fetch(page)

                    if response.status_code in {429, 500, 502, 503, 504} and attempt < self.retries:
                        await asyncio.sleep(1.5 * attempt)
                        continue

                    return page
                except httpx.HTTPError as exc:
                    last_error = exc
                    if attempt < self.retries:
                        await asyncio.sleep(1.5 * attempt)
                        continue

        assert last_error is not None
        raise last_error
