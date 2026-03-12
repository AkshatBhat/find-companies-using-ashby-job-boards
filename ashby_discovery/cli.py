from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path

from .config import RunConfig
from .pipeline import run_pipeline

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover and verify company slugs for Ashby-hosted job boards",
    )
    parser.add_argument("--max-results", type=int, default=500, help="Maximum verified rows to write")
    parser.add_argument(
        "--input-company-list",
        type=Path,
        default=None,
        help="Optional text file of company domains/URLs to scan for Ashby embeds",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for CSV/JSON output and state",
    )
    parser.add_argument("--db-path", type=Path, default=None, help="SQLite DB path for resume/cache state")
    parser.add_argument("--resume", action="store_true", help="Resume from existing SQLite state if available")
    parser.add_argument(
        "--search-pages-per-query",
        type=int,
        default=5,
        help="How many paginated search result pages to fetch per query",
    )
    parser.add_argument(
        "--search-engines",
        type=str,
        default="duckduckgo,brave,yahoo",
        help="Comma-separated search providers in priority order (supported: duckduckgo, brave, yahoo)",
    )
    parser.add_argument(
        "--search-queries-file",
        type=Path,
        default=None,
        help="Optional text file with one search query per line",
    )
    parser.add_argument(
        "--source-scan-batch-size",
        type=int,
        default=25,
        help="How many source pages to scan per batch",
    )
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout (seconds)")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry attempts")
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.25,
        help="Minimum delay between requests (seconds)",
    )
    parser.add_argument("--max-connections", type=int, default=20, help="Max concurrent HTTP connections")
    parser.add_argument(
        "--user-agent",
        type=str,
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent string for discovery requests",
    )
    parser.add_argument(
        "--include-unverified",
        action="store_true",
        help="Include NOT_VERIFIED/ERROR rows in output (default writes VERIFIED only)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print debug logs to stdout while the pipeline runs",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars",
    )
    return parser


def resolve_db_path(output_dir: Path, db_path_arg: Path | None, resume: bool) -> Path:
    if db_path_arg is not None:
        if resume:
            return db_path_arg
        if db_path_arg.exists():
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            return db_path_arg.with_name(f"{db_path_arg.stem}_{timestamp}{db_path_arg.suffix}")
        return db_path_arg

    default = output_dir / "discovery_state.sqlite"
    if resume or not default.exists():
        return default

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return output_dir / f"discovery_state_{timestamp}.sqlite"


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = resolve_db_path(output_dir, args.db_path, args.resume)

    search_engines = [value.strip() for value in args.search_engines.split(",") if value.strip()]
    show_progress = not args.no_progress

    if show_progress:
        try:
            import tqdm  # noqa: F401
        except Exception:
            print("Note: `tqdm` is not installed in this environment; progress bars will not be shown.")
            print("Install dependencies with: pip install -r requirements.txt")

    config = RunConfig(
        output_dir=output_dir,
        db_path=db_path,
        max_results=args.max_results,
        input_company_list=args.input_company_list,
        resume=args.resume,
        search_pages_per_query=args.search_pages_per_query,
        search_engines=search_engines,
        search_queries_file=args.search_queries_file,
        source_scan_batch_size=args.source_scan_batch_size,
        timeout_seconds=args.timeout,
        retries=args.retries,
        min_interval_seconds=args.request_delay,
        max_connections=args.max_connections,
        verified_only=not args.include_unverified,
        verbose=args.verbose,
        show_progress=show_progress,
        user_agent=args.user_agent,
    )

    summary = asyncio.run(run_pipeline(config))

    print("Discovery complete")
    print(f"- SQLite state: {db_path}")
    print(f"- Candidates added: {summary['discovery']['candidates_added']}")
    print(f"- Source pages scanned: {summary['discovery']['source_pages_scanned']}")
    print(f"- Slugs verified: {summary['verification']['verified']} / {summary['verification']['processed']}")
    if args.resume:
        print(
            f"- Rows exported (cumulative): {summary['output']['rows_written']} "
            f"(this run: {summary['run_counts']['rows_written']})"
        )
        print(
            f"- Failures recorded (cumulative): {summary['store_counts']['failures']} "
            f"(this run: {summary['run_counts']['failures']})"
        )
    else:
        print(f"- Rows exported: {summary['output']['rows_written']}")
        print(f"- Failures recorded: {summary['store_counts']['failures']}")
    print(f"- CSV: {summary['output']['csv']}")
    print(f"- JSON: {summary['output']['json']}")
    print(f"- Failures log: {summary['output']['failures']}")

    if summary["discovery"]["candidates_added"] == 0:
        if summary["store_counts"]["candidates"] > 0:
            print("- Note: no NEW candidates were added in this run (existing state already contains candidates).")
            print("- Tip: run without --resume for a fresh DB, or broaden inputs/queries for new discoveries.")
        else:
            print("- Warning: no candidates discovered.")
            print("- Hint: inspect failures.jsonl and consider using --input-company-list with company domains.")


if __name__ == "__main__":
    main()
