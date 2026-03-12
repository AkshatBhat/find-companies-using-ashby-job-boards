from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence


@dataclass
class RunConfig:
    output_dir: Path
    db_path: Path
    max_results: int = 500
    input_company_list: Optional[Path] = None
    resume: bool = False
    search_pages_per_query: int = 5
    search_engines: Sequence[str] = ("duckduckgo", "brave", "yahoo")
    search_queries_file: Optional[Path] = None
    source_scan_batch_size: int = 25
    timeout_seconds: float = 15.0
    retries: int = 3
    min_interval_seconds: float = 0.25
    max_connections: int = 20
    verify_limit_multiplier: int = 4
    verified_only: bool = True
    verbose: bool = False
    show_progress: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
