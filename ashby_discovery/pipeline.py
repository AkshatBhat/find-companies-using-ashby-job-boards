from __future__ import annotations

import asyncio
import sys
from typing import Any

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

from .config import RunConfig
from .discovery import discover_candidates
from .enrichment import enrich_company_name
from .http_client import AsyncFetcher
from .logging_utils import debug_log
from .output import save_failures, save_results
from .storage import DiscoveryStore
from .verification import verify_slug

async def _verify_many(
    fetcher: AsyncFetcher,
    store: DiscoveryStore,
    slugs: list[str],
    *,
    verbose: bool = False,
    show_progress: bool = True,
    max_in_flight: int = 20,
    persist_batch_size: int = 50,
) -> tuple[int, int]:
    verified = 0
    processed = 0

    if not slugs:
        return processed, verified

    max_in_flight = max(1, min(max_in_flight, len(slugs)))
    progress = tqdm(
        total=len(slugs),
        desc="Verifying slugs",
        unit="slug",
        disable=not show_progress,
        file=sys.stdout,
        dynamic_ncols=True,
        leave=True,
    )

    pending_results = []
    pending_failures: list[tuple[str, str, str]] = []
    slug_iter = iter(slugs)
    in_flight: set[asyncio.Task] = set()

    def _flush_persistence() -> None:
        nonlocal pending_results, pending_failures
        if not pending_results and not pending_failures:
            return
        with store.batch_write():
            if pending_results:
                store.save_verifications(pending_results)
            if pending_failures:
                store.record_failures(pending_failures)
        pending_results = []
        pending_failures = []

    try:
        for _ in range(max_in_flight):
            try:
                slug = next(slug_iter)
            except StopIteration:
                break
            in_flight.add(asyncio.create_task(verify_slug(fetcher, slug)))

        while in_flight:
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                in_flight.remove(task)
                result = task.result()
                processed += 1
                if progress is not None:
                    progress.update(1)
                result = enrich_company_name(result, store)
                pending_results.append(result)
                if result.verification_status == "VERIFIED":
                    verified += 1
                elif result.verification_status == "ERROR":
                    pending_failures.append(("verify_slug", result.slug, result.notes))

                if verbose and processed % 50 == 0:
                    debug_log(f"Verified {processed}/{len(slugs)} slugs so far", verbose=verbose)

                if len(pending_results) >= persist_batch_size or len(pending_failures) >= persist_batch_size:
                    _flush_persistence()

                try:
                    slug = next(slug_iter)
                except StopIteration:
                    continue
                in_flight.add(asyncio.create_task(verify_slug(fetcher, slug)))
        _flush_persistence()
    finally:
        _flush_persistence()
        if progress is not None:
            progress.close()

    return processed, verified


async def run_pipeline(config: RunConfig) -> dict[str, Any]:
    debug_log(f"Opening discovery store at {config.db_path}", verbose=config.verbose)
    store = DiscoveryStore(config.db_path)
    initial_store_counts = store.counts()
    initial_rows_written = min(store.count_export_results(verified_only=config.verified_only), config.max_results)
    if config.verbose:
        debug_log(f"SQLite index health: {store.index_health()}", verbose=config.verbose)
    fetcher = AsyncFetcher(
        store,
        timeout_seconds=config.timeout_seconds,
        retries=config.retries,
        min_interval_seconds=config.min_interval_seconds,
        max_connections=config.max_connections,
        user_agent=config.user_agent,
    )

    try:
        debug_log("Starting candidate discovery", verbose=config.verbose)
        discovery_summary = await discover_candidates(
            fetcher,
            store,
            max_results=config.max_results,
            input_company_list=config.input_company_list,
            search_pages_per_query=config.search_pages_per_query,
            search_engines=config.search_engines,
            search_queries_file=config.search_queries_file,
            source_scan_batch_size=config.source_scan_batch_size,
            verbose=config.verbose,
            show_progress=config.show_progress,
        )
        debug_log(
            (
                "Discovery finished: "
                f"candidates_added={discovery_summary.candidates_added}, "
                f"sources_scanned={discovery_summary.source_pages_scanned}"
            ),
            verbose=config.verbose,
        )

        verify_limit = config.max_results * config.verify_limit_multiplier
        slugs_to_verify = store.list_unverified_slugs(limit=verify_limit)
        debug_log(f"Starting verification for {len(slugs_to_verify)} unverified slugs", verbose=config.verbose)
        processed, verified = await _verify_many(
            fetcher,
            store,
            slugs_to_verify,
            verbose=config.verbose,
            show_progress=config.show_progress,
            max_in_flight=config.max_connections,
        )
        debug_log(f"Verification finished: verified={verified}, processed={processed}", verbose=config.verbose)

        rows = store.export_results(verified_only=config.verified_only)
        rows = rows[: config.max_results]
        rows_written_this_run = max(0, len(rows) - initial_rows_written)
        if config.resume:
            debug_log(
                f"Exporting {len(rows)} cumulative rows to CSV/JSON (this run: +{rows_written_this_run})",
                verbose=config.verbose,
            )
        else:
            debug_log(f"Exporting {len(rows)} rows to CSV/JSON", verbose=config.verbose)

        csv_path, json_path = save_results(rows, config.output_dir)
        failures = store.export_failures()
        failures_this_run = max(0, len(failures) - initial_store_counts.get("failures", 0))
        failure_path = save_failures(failures, config.output_dir)
        if config.resume:
            debug_log(
                f"Recorded {len(failures)} cumulative failures (this run: +{failures_this_run})",
                verbose=config.verbose,
            )
        else:
            debug_log(f"Recorded {len(failures)} failures", verbose=config.verbose)

        final_store_counts = store.counts()

        return {
            "discovery": {
                "candidates_added": discovery_summary.candidates_added,
                "source_pages_enqueued": discovery_summary.source_pages_enqueued,
                "source_pages_scanned": discovery_summary.source_pages_scanned,
            },
            "verification": {
                "processed": processed,
                "verified": verified,
            },
            "store_counts": final_store_counts,
            "run_counts": {
                "rows_written": rows_written_this_run,
                "failures": max(0, final_store_counts.get("failures", 0) - initial_store_counts.get("failures", 0)),
            },
            "output": {
                "csv": str(csv_path),
                "json": str(json_path),
                "failures": str(failure_path),
                "rows_written": len(rows),
            },
        }
    finally:
        await fetcher.close()
        store.close()
