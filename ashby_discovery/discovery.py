from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

from bs4 import BeautifulSoup
try:
    from tqdm.auto import tqdm
except Exception:  # pragma: no cover
    class tqdm:  # type: ignore[no-redef]
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return

        def update(self, _: int = 1) -> None:
            return

        def close(self) -> None:
            return

from .extractors import extract_slug, extract_slugs_from_html, has_embed_marker
from .http_client import AsyncFetcher
from .logging_utils import debug_log
from .models import CandidateSlug
from .search_providers import (
    DEFAULT_SEARCH_ENGINES,
    SUPPORTED_SEARCH_ENGINES,
    blocked_reason,
    search_urls_for,
    select_result_links,
)
from .storage import DiscoveryStore
from .utils import extract_urls_from_text, maybe_unwrap_search_redirect, uniq

FALLBACK_SEARCH_QUERIES = [
    'site:jobs.ashbyhq.com',
    '"jobs.ashbyhq.com" careers',
    '"jobs.ashbyhq.com" "software engineer"',
    '"jobs.ashbyhq.com" "embed?version=2"',
    '"__ashbyBaseJobBoardUrl"',
]
DEFAULT_SEARCH_QUERIES_FILE = Path(__file__).resolve().parent.parent / "search_queries.txt"

CAREER_PATH_HINTS = [
    "/careers",
    "/jobs",
    "/company/careers",
    "/about/careers",
]


@dataclass
class DiscoverySummary:
    candidates_added: int
    source_pages_enqueued: int
    source_pages_scanned: int


@dataclass
class ProviderSearchResult:
    urls: list[str]
    blocked_reason: Optional[str] = None  # e.g. "rate_limited", "challenge"


def load_company_seeds(path: Optional[Path]) -> list[str]:
    if path is None:
        return []
    if not path.exists():
        raise FileNotFoundError(f"Input company list file does not exist: {path}")

    seeds: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        seeds.append(value)
    return uniq(seeds)


def load_search_queries(path: Optional[Path] = None) -> list[str]:
    target = path if path is not None else DEFAULT_SEARCH_QUERIES_FILE
    if target is not None and not target.exists() and path is None:
        # Root-level query file is optional; fall back to in-code defaults.
        target = None
    elif target is not None and not target.exists():
        target = None

    if target is None:
        return FALLBACK_SEARCH_QUERIES

    queries: list[str] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        queries.append(value)
    return uniq(queries) or FALLBACK_SEARCH_QUERIES


def expand_company_seed(seed: str) -> list[str]:
    if seed.startswith("http://") or seed.startswith("https://"):
        parsed = urlparse(seed)
        if parsed.netloc.lower() == "jobs.ashbyhq.com":
            return [seed]
        base = f"{parsed.scheme}://{parsed.netloc}"
        return uniq([seed, base] + [f"{base}{path}" for path in CAREER_PATH_HINTS])

    # Domain-only inputs like "acme.com"
    base = f"https://{seed}"
    return uniq([base] + [f"{base}{path}" for path in CAREER_PATH_HINTS])


def normalize_search_engines(engines: Optional[Iterable[str]]) -> list[str]:
    if engines is None:
        return list(DEFAULT_SEARCH_ENGINES)

    alias_map = {"ddg": "duckduckgo"}
    out: list[str] = []
    for raw in engines:
        normalized = alias_map.get(raw.strip().lower(), raw.strip().lower())
        if not normalized:
            continue
        if normalized not in SUPPORTED_SEARCH_ENGINES:
            continue
        if normalized in out:
            continue
        out.append(normalized)

    return out or list(DEFAULT_SEARCH_ENGINES)


def _is_external_result_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = parsed.netloc.lower()
    if not host:
        return False

    blocked_hosts = (
        "duckduckgo.com",
        "duck.com",
        "search.brave.com",
        "yahoo.com",
        "w3.org",
    )
    if any(host.endswith(blocked) for blocked in blocked_hosts):
        return False

    blocked_extensions = (
        ".css",
        ".js",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".woff",
        ".woff2",
        ".ico",
        ".xml",
        ".webp",
    )
    if parsed.path.lower().endswith(blocked_extensions):
        return False
    return True


def _unwrap_possible_search_redirect(url: str) -> str:
    unwrapped = maybe_unwrap_search_redirect(url)
    parsed = urlparse(unwrapped)
    query = parse_qs(parsed.query)
    for key in ("url", "u", "uddg", "target", "q"):
        values = query.get(key)
        if not values:
            continue
        candidate = unquote(values[0])
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
    return unwrapped


async def _search_provider(
    fetcher: AsyncFetcher,
    query: str,
    *,
    engine: str,
    pages: int = 5,
    store: Optional[DiscoveryStore] = None,
    verbose: bool = False,
    progress: Optional[Any] = None,
) -> ProviderSearchResult:
    discovered: list[str] = []
    provider_blocked_reason: Optional[str] = None
    attempted_pages = 0
    for page_idx in range(pages):
        attempted_pages = page_idx + 1
        candidate_urls = search_urls_for(engine, query, page_idx)
        page_blocked_reason: Optional[str] = None
        page_success = False
        try:
            for url in candidate_urls:
                try:
                    # Avoid reusing stale anti-bot pages from cache.
                    page = await fetcher.fetch(url, use_cache=False)
                except Exception as exc:  # noqa: BLE001
                    if store is not None:
                        store.record_failure(f"search_discovery:{engine}", url, str(exc))
                    continue

                reason = blocked_reason(engine, page.status_code, page.text)
                if reason is not None:
                    if store is not None:
                        store.record_failure(f"search_discovery:{engine}", url, "Search provider challenge/blocked page")
                    page_blocked_reason = reason
                    # Try alternate endpoint (e.g. DDG lite) before giving up this page.
                    continue

                if page.status_code >= 400:
                    if store is not None:
                        store.record_failure(f"search_discovery:{engine}", url, f"HTTP {page.status_code}")
                    continue

                soup = BeautifulSoup(page.text, "html.parser")
                page_discovered: list[str] = []
                for href in select_result_links(engine, soup):
                    unwrapped = _unwrap_possible_search_redirect(href)
                    if _is_external_result_url(unwrapped):
                        page_discovered.append(unwrapped)

                # Fallback parser if result CSS classes changed.
                if not page_discovered:
                    for raw_url in extract_urls_from_text(page.text):
                        unwrapped = _unwrap_possible_search_redirect(raw_url)
                        if _is_external_result_url(unwrapped):
                            page_discovered.append(unwrapped)

                discovered.extend(page_discovered)
                page_success = True
                page_blocked_reason = None

                # DDG can return empty pages on one endpoint; try alternate endpoint for this page.
                if engine == "duckduckgo" and not page_discovered:
                    continue
                break

            if not page_success and page_blocked_reason is not None:
                provider_blocked_reason = page_blocked_reason
                break
        finally:
            if progress is not None:
                progress.update(1)

    if provider_blocked_reason is not None and progress is not None and attempted_pages < pages:
        progress.update(pages - attempted_pages)

    return ProviderSearchResult(urls=uniq(discovered), blocked_reason=provider_blocked_reason)


async def discover_from_search(
    fetcher: AsyncFetcher,
    max_results: int,
    *,
    search_pages_per_query: int,
    search_engines: Optional[Iterable[str]] = None,
    search_queries_file: Optional[Path] = None,
    store: Optional[DiscoveryStore] = None,
    queries: Optional[list[str]] = None,
    verbose: bool = False,
    show_progress: bool = True,
) -> list[str]:
    if search_pages_per_query <= 0:
        debug_log("Search discovery skipped because --search-pages-per-query=0", verbose=verbose)
        return []

    query_list = queries or load_search_queries(search_queries_file)
    engine_list = normalize_search_engines(search_engines)
    urls: list[str] = []
    engine_next_allowed: dict[str, float] = {}
    engine_penalty_level: dict[str, int] = {engine: 0 for engine in engine_list}
    engine_block_count: dict[str, int] = {engine: 0 for engine in engine_list}
    disabled_engines: set[str] = set()
    cooldown_base = 20.0
    cooldown_max = 300.0
    disable_after_block_count = 10
    total_search_pages = len(query_list) * len(engine_list) * search_pages_per_query
    progress = (
        tqdm(
            total=total_search_pages,
            desc="Search pages",
            unit="page",
            disable=not show_progress,
            file=sys.stdout,
            dynamic_ncols=True,
            leave=True,
        )
        if total_search_pages > 0
        else None
    )

    debug_log(
        f"Starting search discovery with engines={','.join(engine_list)} queries={len(query_list)}",
        verbose=verbose,
    )
    try:
        loop = asyncio.get_running_loop()
        for query in query_list:
            query_results: list[str] = []
            debug_log(f"[search] running query: {query}", verbose=verbose)
            for engine in engine_list:
                if engine in disabled_engines:
                    if progress is not None:
                        progress.update(search_pages_per_query)
                    debug_log(
                        f"[search] query={query!r} engine={engine} skipped (disabled for current run)",
                        verbose=verbose,
                    )
                    continue

                now = loop.time()
                next_allowed = engine_next_allowed.get(engine, 0.0)
                if now < next_allowed:
                    if progress is not None:
                        progress.update(search_pages_per_query)
                    wait_s = int(next_allowed - now)
                    debug_log(
                        f"[search] query={query!r} engine={engine} skipped due to cooldown ({wait_s}s)",
                        verbose=verbose,
                    )
                    continue

                search_result = await _search_provider(
                    fetcher,
                    query,
                    engine=engine,
                    pages=search_pages_per_query,
                    store=store,
                    verbose=verbose,
                    progress=progress,
                )
                query_results.extend(search_result.urls)
                urls.extend(search_result.urls)
                debug_log(
                    f"[search] query={query!r} engine={engine} cumulative URLs={len(query_results)}",
                    verbose=verbose,
                )

                if search_result.blocked_reason is not None:
                    engine_block_count[engine] = min(engine_block_count[engine] + 1, disable_after_block_count)
                    engine_penalty_level[engine] = min(engine_penalty_level[engine] + 1, 6)
                    cooldown = min(cooldown_base * (2 ** (engine_penalty_level[engine] - 1)), cooldown_max)
                    engine_next_allowed[engine] = loop.time() + cooldown
                    debug_log(
                        f"[search] engine={engine} backoff activated ({search_result.blocked_reason}, {int(cooldown)}s)",
                        verbose=verbose,
                    )
                    if engine_block_count[engine] >= disable_after_block_count:
                        disabled_engines.add(engine)
                        debug_log(
                            f"[search] engine={engine} disabled for current run after repeated blocking",
                            verbose=verbose,
                        )
                elif search_result.urls:
                    # Successful pages reset penalty level.
                    engine_block_count[engine] = 0
                    engine_penalty_level[engine] = 0
                    engine_next_allowed[engine] = 0.0
                else:
                    # Non-blocking but empty result: clear block streak.
                    engine_block_count[engine] = 0

                if len(urls) >= max_results:
                    break

            if not query_results and store is not None:
                store.record_failure("search_discovery", query, "No external result URLs extracted across providers")
            if len(urls) >= max_results:
                break
    finally:
        if progress is not None:
            progress.close()
    return uniq(urls)[:max_results]


async def scan_source_page(
    fetcher: AsyncFetcher,
    source_url: str,
    source_type: str,
) -> tuple[list[CandidateSlug], Optional[str]]:
    try:
        page = await fetcher.fetch(source_url)
    except Exception as exc:  # noqa: BLE001
        return [], str(exc)

    if page.status_code >= 400:
        return [], f"HTTP {page.status_code}"

    has_marker = has_embed_marker(page.text)
    found_slugs = extract_slugs_from_html(page.text)
    if not found_slugs and not has_marker:
        return [], None

    note = "Ashby embed marker detected" if has_marker else "Ashby URL detected"
    candidates = [
        CandidateSlug(
            slug=slug,
            source_type="embedded_careers_page",
            source_url=source_url,
            notes=f"{note}; discovered from {source_type}",
        )
        for slug in sorted(found_slugs)
    ]
    return candidates, None


async def discover_candidates(
    fetcher: AsyncFetcher,
    store: DiscoveryStore,
    *,
    max_results: int,
    input_company_list: Optional[Path],
    search_pages_per_query: int = 5,
    search_engines: Optional[Iterable[str]] = None,
    search_queries_file: Optional[Path] = None,
    source_scan_batch_size: int = 30,
    verbose: bool = False,
    show_progress: bool = True,
) -> DiscoverySummary:
    candidate_buffer: list[CandidateSlug] = []
    source_pages_enqueued = 0
    source_pages_scanned = 0

    discovered_urls = await discover_from_search(
        fetcher,
        max_results=max_results * 4,
        search_pages_per_query=search_pages_per_query,
        search_engines=search_engines,
        search_queries_file=search_queries_file,
        store=store,
        verbose=verbose,
        show_progress=show_progress,
    )
    debug_log(f"Search discovery produced {len(discovered_urls)} candidate URLs", verbose=verbose)

    seeds = load_company_seeds(input_company_list)
    if seeds:
        debug_log(f"Loaded {len(seeds)} company seeds from input file", verbose=verbose)

    with store.batch_write():
        for url in discovered_urls:
            slug = extract_slug(url)
            if slug:
                candidate_buffer.append(
                    CandidateSlug(
                        slug=slug,
                        source_type="direct_search",
                        source_url=url,
                        notes="Discovered via search-engine query",
                    )
                )
                continue

            if urlparse(url).scheme in {"http", "https"}:
                store.upsert_source(url, "search_result_page")
                source_pages_enqueued += 1

        for seed in seeds:
            for expanded in expand_company_seed(seed):
                slug = extract_slug(expanded)
                if slug:
                    candidate_buffer.append(
                        CandidateSlug(
                            slug=slug,
                            source_type="other",
                            source_url=expanded,
                            notes="Provided directly via --input-company-list",
                        )
                    )
                else:
                    store.upsert_source(expanded, "input_company_list")
                    source_pages_enqueued += 1

        candidates_added = store.add_candidates(candidate_buffer)
    debug_log(f"Initial candidate insert added {candidates_added} rows", verbose=verbose)

    unscanned_total = store.count_unscanned_sources()
    source_progress = (
        tqdm(
            total=unscanned_total,
            desc="Source pages",
            unit="page",
            disable=not show_progress,
            file=sys.stdout,
            dynamic_ncols=True,
            leave=True,
        )
        if unscanned_total > 0
        else None
    )

    try:
        while True:
            batch = store.get_unscanned_sources(source_scan_batch_size)
            if not batch:
                break

            debug_log(f"Scanning source batch of {len(batch)} pages", verbose=verbose)
            tasks = [scan_source_page(fetcher, url, src_type) for url, src_type in batch]
            results = await asyncio.gather(*tasks)
            with store.batch_write():
                for (url, _src_type), (found_candidates, error) in zip(batch, results):
                    source_pages_scanned += 1
                    if source_progress is not None:
                        source_progress.update(1)
                    if error:
                        store.mark_source_scanned(url, error=error)
                        store.record_failure("source_scan", url, error)
                        debug_log(f"[source_scan] failed: {url} -> {error}", verbose=verbose)
                        continue

                    if found_candidates:
                        inserted = store.add_candidates(found_candidates)
                        candidates_added += inserted
                        debug_log(
                            f"[source_scan] {url} yielded {len(found_candidates)} slugs ({inserted} new)",
                            verbose=verbose,
                        )
                    store.mark_source_scanned(url, error=None)
    finally:
        if source_progress is not None:
            source_progress.close()

    return DiscoverySummary(
        candidates_added=candidates_added,
        source_pages_enqueued=source_pages_enqueued,
        source_pages_scanned=source_pages_scanned,
    )
