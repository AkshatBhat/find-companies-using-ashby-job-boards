from __future__ import annotations

import re
from urllib.parse import quote

from .extractors import infer_company_name
from .http_client import AsyncFetcher
from .models import VerificationResult
from .utils import clean_whitespace

POSITIVE_MARKERS = (
    "jobs.ashbyhq.com",
    "ashbyhq",
    "window.ashby",
    "__ashbybasejobboardurl",
    "embed?version=2",
    "open roles",
    "all teams",
)

NEGATIVE_MARKERS = (
    "404",
    "not found",
    "page could not be found",
    "access denied",
    "temporarily unavailable",
    "just a moment",
    "attention required",
)


def _score_board_html(slug: str, html: str) -> tuple[int, int]:
    lower = html.lower()
    positive = 0
    negative = 0
    encoded_slug = quote(slug, safe="").lower()

    for marker in POSITIVE_MARKERS:
        if marker in lower:
            positive += 1

    for marker in NEGATIVE_MARKERS:
        if marker in lower:
            negative += 1

    if f"jobs.ashbyhq.com/{slug.lower()}" in lower:
        positive += 2
    if f"jobs.ashbyhq.com/{encoded_slug}" in lower:
        positive += 2

    if re.search(rf"/{re.escape(slug)}/job/", lower):
        positive += 2
    if re.search(rf"/{re.escape(encoded_slug)}/job/", lower):
        positive += 2

    return positive, negative


async def verify_slug(fetcher: AsyncFetcher, slug: str) -> VerificationResult:
    encoded_slug = quote(slug, safe="")
    ashby_url = f"https://jobs.ashbyhq.com/{encoded_slug}"
    try:
        page = await fetcher.fetch(ashby_url, use_cache=False)
    except Exception as exc:  # noqa: BLE001
        return VerificationResult(
            slug=slug,
            ashby_url=ashby_url,
            verification_status="ERROR",
            notes=f"Request failed: {clean_whitespace(str(exc))}",
        )

    if page.status_code != 200:
        return VerificationResult(
            slug=slug,
            ashby_url=ashby_url,
            verification_status="NOT_VERIFIED",
            http_status=page.status_code,
            notes=f"HTTP {page.status_code}",
        )

    positive, negative = _score_board_html(slug, page.text)
    company = infer_company_name(page.text)

    if positive >= 3 and negative <= 2:
        return VerificationResult(
            slug=slug,
            ashby_url=ashby_url,
            verification_status="VERIFIED",
            inferred_company_name=company,
            http_status=page.status_code,
            notes=f"Strong Ashby board indicators detected (score={positive}, negatives={negative})",
        )

    return VerificationResult(
        slug=slug,
        ashby_url=ashby_url,
        verification_status="NOT_VERIFIED",
        inferred_company_name=company,
        http_status=page.status_code,
        notes=f"Weak board indicators (score={positive}, negatives={negative})",
    )
