from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import pytest

from ashby_discovery.config import RunConfig
from ashby_discovery.models import FetchedPage
from ashby_discovery.pipeline import run_pipeline


class ReplayFetcher:
    def __init__(self, *_args, **_kwargs) -> None:  # noqa: ANN002
        return

    async def fetch(self, url: str, *, use_cache: bool = True) -> FetchedPage:  # noqa: ARG002
        if url.endswith("/acme"):
            html = """
            <html>
              <head><title>Acme - Jobs</title></head>
              <body>
                <a href="https://jobs.ashbyhq.com/acme/job/123">Role</a>
                <script>window.Ashby = {};</script>
                <div>Open roles</div>
              </body>
            </html>
            """
            return FetchedPage(
                url=url,
                final_url=url,
                status_code=200,
                text=html,
                fetched_at=datetime.utcnow(),
            )

        if url.endswith("/ghost"):
            return FetchedPage(
                url=url,
                final_url=url,
                status_code=404,
                text="Not Found",
                fetched_at=datetime.utcnow(),
            )

        raise RuntimeError(f"Unexpected URL in replay fetcher: {url}")

    async def close(self) -> None:
        return


@pytest.mark.asyncio
async def test_pipeline_deterministic_output_and_resume_deltas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ashby_discovery.pipeline.AsyncFetcher", ReplayFetcher)

    seed_file = tmp_path / "seed_list.txt"
    seed_file.write_text(
        "\n".join(
            [
                "https://jobs.ashbyhq.com/acme",
                "https://jobs.ashbyhq.com/ghost",
            ]
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "output"
    db_path = output_dir / "discovery_state.sqlite"
    cfg = RunConfig(
        output_dir=output_dir,
        db_path=db_path,
        max_results=50,
        input_company_list=seed_file,
        resume=True,
        search_pages_per_query=0,
        verified_only=False,
        show_progress=False,
    )

    first = await run_pipeline(cfg)
    second = await run_pipeline(cfg)

    # Resume deltas: second run should export the same cumulative set, but add nothing new.
    assert first["output"]["rows_written"] == 2
    assert first["run_counts"]["rows_written"] == 2
    assert second["output"]["rows_written"] == 2
    assert second["run_counts"]["rows_written"] == 0

    # Failure counters remain stable in replay fixture.
    assert first["store_counts"]["failures"] == 0
    assert second["store_counts"]["failures"] == 0
    assert second["run_counts"]["failures"] == 0

    csv_path = Path(second["output"]["csv"])
    json_path = Path(second["output"]["json"])
    csv_rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    json_rows = json.loads(json_path.read_text(encoding="utf-8"))

    # Exact output match across CSV/JSON after stable order by slug.
    expected = [
        {
            "slug": "acme",
            "inferred_company_name": "Acme",
            "ashby_url": "https://jobs.ashbyhq.com/acme",
                "source_type": "other",
                "source_url": "https://jobs.ashbyhq.com/acme",
                "verification_status": "VERIFIED",
                "notes": "Strong Ashby board indicators detected (score=12, negatives=0)",
            },
        {
            "slug": "ghost",
            "inferred_company_name": "",
            "ashby_url": "https://jobs.ashbyhq.com/ghost",
            "source_type": "other",
            "source_url": "https://jobs.ashbyhq.com/ghost",
            "verification_status": "NOT_VERIFIED",
            "notes": "HTTP 404",
        },
    ]

    assert csv_rows == expected
    assert json_rows == expected
