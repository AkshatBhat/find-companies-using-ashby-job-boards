from __future__ import annotations

import json
import re
from typing import Optional

from bs4 import BeautifulSoup

from .utils import URL_SLUG_REGEX, clean_whitespace, extract_slug_from_url, normalize_slug

EMBED_MARKERS = (
    "jobs.ashbyhq.com",
    "/embed?version=2",
    "__ashbyBaseJobBoardUrl",
    "window.Ashby",
    "window.ashby",
)

ASHBY_URL_IN_TEXT = re.compile(
    r"https?://jobs\.ashbyhq\.com/([^/?#\"'<>]+)(?:[/?#]|$)",
    flags=re.IGNORECASE,
)

ASHBY_BASE_URL_ASSIGNMENT = re.compile(
    r"__ashbyBaseJobBoardUrl\s*[:=]\s*[\"']https?://jobs\.ashbyhq\.com/([^/?#\"'<>]+)",
    flags=re.IGNORECASE,
)


def extract_slug(raw: str) -> Optional[str]:
    """Extract a slug from either a full Ashby URL or a slug-like token."""
    if raw.startswith("http://") or raw.startswith("https://"):
        return extract_slug_from_url(raw)
    return normalize_slug(raw)


def has_embed_marker(html: str) -> bool:
    lower = html.lower()
    return any(marker.lower() in lower for marker in EMBED_MARKERS)


def extract_slugs_from_html(html: str) -> set[str]:
    slugs: set[str] = set()

    for match in ASHBY_URL_IN_TEXT.finditer(html):
        slug = normalize_slug(match.group(1))
        if slug:
            slugs.add(slug)

    for match in ASHBY_BASE_URL_ASSIGNMENT.finditer(html):
        slug = normalize_slug(match.group(1))
        if slug:
            slugs.add(slug)

    for match in URL_SLUG_REGEX.finditer(html):
        slug = normalize_slug(match.group(1))
        if slug:
            slugs.add(slug)

    soup = BeautifulSoup(html, "html.parser")
    for node in soup.find_all(["script", "iframe", "a"]):
        for attr in ("src", "href", "data-url", "data-src"):
            value = node.get(attr)
            if not value:
                continue
            slug = extract_slug(value)
            if slug:
                slugs.add(slug)

    return slugs


def infer_company_name(html: str, fallback: Optional[str] = None) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    og_site_name = soup.find("meta", attrs={"property": "og:site_name"})
    if og_site_name and og_site_name.get("content"):
        return clean_whitespace(og_site_name["content"])

    title = soup.title.string if soup.title and soup.title.string else ""
    if title:
        # Titles are frequently like: "Acme - Jobs"
        cleaned = clean_whitespace(title)
        for sep in (" - ", " | ", " — ", " – "):
            if sep in cleaned:
                left = cleaned.split(sep)[0].strip()
                if left:
                    return left
        return cleaned

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not script.string:
            continue
        try:
            payload = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        values = payload if isinstance(payload, list) else [payload]
        for value in values:
            if isinstance(value, dict):
                name = value.get("name") or value.get("hiringOrganization", {}).get("name")
                if isinstance(name, str) and name.strip():
                    return clean_whitespace(name)

    return fallback
