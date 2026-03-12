from __future__ import annotations

from pathlib import Path

from ashby_discovery.models import CandidateSlug, VerificationResult
from ashby_discovery.storage import DiscoveryStore


def test_count_export_results_matches_export_rows(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "state.sqlite")
    try:
        with store.batch_write():
            store.add_candidates(
                [
                    CandidateSlug(slug="acme", source_type="direct_search", source_url="https://jobs.ashbyhq.com/acme"),
                    CandidateSlug(slug="ghost", source_type="direct_search", source_url="https://jobs.ashbyhq.com/ghost"),
                ]
            )
            store.save_verifications(
                [
                    VerificationResult(
                        slug="acme",
                        ashby_url="https://jobs.ashbyhq.com/acme",
                        verification_status="VERIFIED",
                    ),
                    VerificationResult(
                        slug="ghost",
                        ashby_url="https://jobs.ashbyhq.com/ghost",
                        verification_status="NOT_VERIFIED",
                    ),
                ]
            )

        assert store.count_export_results(verified_only=True) == len(store.export_results(verified_only=True))
        assert store.count_export_results(verified_only=False) == len(store.export_results(verified_only=False))
    finally:
        store.close()


def test_index_health_returns_expected_keys(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "state.sqlite")
    try:
        health = store.index_health()
        assert set(health.keys()) == {"sources_queue", "unverified_slugs", "verified_export_order"}
        assert all(isinstance(value, bool) for value in health.values())
    finally:
        store.close()

