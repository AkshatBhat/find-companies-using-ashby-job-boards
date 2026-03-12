from __future__ import annotations

from urllib.parse import urlparse

from .models import VerificationResult
from .storage import DiscoveryStore


def enrich_company_name(result: VerificationResult, store: DiscoveryStore) -> VerificationResult:
    """Best-effort enrichment when board page parsing did not produce a clear name."""
    if result.inferred_company_name:
        return result

    sources = store.get_sources_for_slug(result.slug)
    if not sources:
        return result

    for row in sources:
        url = row["source_url"]
        parsed = urlparse(url)
        if parsed.netloc and parsed.netloc != "jobs.ashbyhq.com":
            host = parsed.netloc.lower().replace("www.", "")
            inferred = host.split(".")[0]
            if inferred:
                result.inferred_company_name = inferred
                result.notes = (result.notes + "; name inferred from source domain").strip("; ")
                return result

    return result
