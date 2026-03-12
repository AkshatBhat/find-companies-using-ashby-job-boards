from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable


def save_results(rows: Iterable[dict], output_dir: Path, *, base_name: str = "verified_ashby_slugs") -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_list = rows if isinstance(rows, list) else list(rows)

    csv_path = output_dir / f"{base_name}.csv"
    json_path = output_dir / f"{base_name}.json"

    fieldnames = [
        "slug",
        "inferred_company_name",
        "ashby_url",
        "source_type",
        "source_url",
        "verification_status",
        "notes",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_list:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(rows_list, fh, indent=2, ensure_ascii=False)

    return csv_path, json_path


def save_failures(failures: list[dict], output_dir: Path, *, filename: str = "failures.jsonl") -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / filename
    with target.open("w", encoding="utf-8") as fh:
        for failure in failures:
            fh.write(json.dumps(failure, ensure_ascii=False) + "\n")
    return target
