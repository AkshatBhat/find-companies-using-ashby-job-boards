from __future__ import annotations

from datetime import datetime


def debug_log(message: str, *, verbose: bool) -> None:
    if not verbose:
        return
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

