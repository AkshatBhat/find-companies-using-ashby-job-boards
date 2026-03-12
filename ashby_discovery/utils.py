from __future__ import annotations

import re
from typing import Iterable, Iterator, Optional
from urllib.parse import parse_qs, unquote, urlparse

ASHBY_HOST = "jobs.ashbyhq.com"
STRICT_SLUG_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")
RELAXED_SLUG_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_. -]{0,127}$")
URL_SLUG_REGEX = re.compile(
    r"https?://jobs\.ashbyhq\.com/([^/?#\"'<>]+)(?:[/?#]|$)",
    flags=re.IGNORECASE,
)


def clean_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_slug(slug: str) -> Optional[str]:
    slug = unquote(slug).strip().strip("/")
    if not slug:
        return None
    slug = re.sub(r"\s+", " ", slug)
    if STRICT_SLUG_REGEX.fullmatch(slug) is None and RELAXED_SLUG_REGEX.fullmatch(slug) is None:
        return None
    return slug


def extract_slug_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if parsed.netloc.lower() != ASHBY_HOST:
        return None
    path = parsed.path.strip("/")
    if not path:
        return None
    slug = path.split("/")[0]
    return normalize_slug(slug)


def extract_urls_from_text(text: str) -> Iterator[str]:
    for match in re.finditer(r"https?://[^\s\"'<>]+", text):
        yield match.group(0).rstrip(").,;")


def maybe_unwrap_search_redirect(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        query = parse_qs(parsed.query)
        uddg = query.get("uddg")
        if uddg:
            return unquote(uddg[0])
    return url


def uniq(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out
