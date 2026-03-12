# 🚀 Discover Companies That Use Ashby for Their Careers Portals

This project discovers companies that use `jobs.ashbyhq.com` for their careers/job portals.
It finds and verifies company names (slugs) for Ashby-hosted job boards:

- `https://jobs.ashbyhq.com/{slug}`

This project is built for real discovery + verification, not slug guessing.

## ✨ Highlights

- Finds candidates from multiple sources:
  - Search results that mention `jobs.ashbyhq.com`
  - Careers pages that embed Ashby (`window.Ashby`, `__ashbyBaseJobBoardUrl`, `/embed?version=2`)
- Exports verified records by default.
- Supports resume with SQLite state and dedup.
- Includes retries, timeouts, rate limiting, and verbose logs.
- Search-provider backoff:
  - Starts at `20s`
  - Doubles up to `300s`
  - Disables provider for the run after `10` blocked events

## 🧰 Tech Stack

- Python 3
- `httpx` (async HTTP)
- `beautifulsoup4` (HTML parsing)
- SQLite (`sqlite3`) for state and cache
- `tqdm` (progress bars)
- `pytest` + `pytest-asyncio` (tests)

## 🗂️ Repository Layout

```text
ashby_discovery/
  __main__.py
  cli.py
  config.py
  pipeline.py
  discovery.py
  extractors.py
  verification.py
  enrichment.py
  http_client.py
  search_providers.py
  logging_utils.py
  storage.py
  output.py
  models.py
  utils.py
docs/
  ARCHITECTURE.md
  CLI_REFERENCE.md
  SQLITE_SCHEMA.md
  OPERATIONS.md
  diagrams/
    high-level-system-diagram.png
    in-depth-system-diagram.png
examples/
  example_verified_ashby_slugs.csv
search_queries.txt
seed_list.txt
requirements.txt
tests/
```

## 🖼️ System Diagram (High-Level)

Path: `docs/diagrams/high-level-system-diagram.png`

![High-level system diagram](docs/diagrams/high-level-system-diagram.png)

## ⚡ Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run:

```bash
python -m ashby_discovery --max-results 500 --output-dir output
```

## 🧪 Common Commands

Verbose run:

```bash
python -m ashby_discovery --max-results 500 --output-dir output --verbose
```

Resume run:

```bash
python -m ashby_discovery --max-results 500 --output-dir output --resume
```

Seed-only mode (skip search):

```bash
python -m ashby_discovery \
  --input-company-list seed_list.txt \
  --output-dir output \
  --resume \
  --search-pages-per-query 0
```

Custom search query file:

```bash
python -m ashby_discovery \
  --output-dir output \
  --search-queries-file search_queries.txt
```

## 📝 Inputs

- `search_queries.txt`: default query list (`#` comments supported)
- `--input-company-list`: optional domains/URLs (one per line)
- Supported engines: `duckduckgo`, `brave`, `yahoo`

## 📦 Outputs

Inside `--output-dir`:

- `verified_ashby_slugs.csv`
- `verified_ashby_slugs.json`
- `failures.jsonl`
- `discovery_state.sqlite` (or timestamped DB for non-resume runs)

CSV/JSON fields:

- `slug`
- `inferred_company_name`
- `ashby_url`
- `source_type`
- `source_url`
- `verification_status`
- `notes`

## 🔁 Resume Semantics

With `--resume`, summary counts are cumulative and include this-run deltas:

- `Rows exported (cumulative): X (this run: Y)`
- `Failures recorded (cumulative): X (this run: Y)`

## 🎯 `--max-results` Behavior

`--max-results` affects more than export:

- Final CSV/JSON rows are capped at `max_results`.
- Search URL collection budget is `max_results * 4`.
- Verification selection budget is `max_results * 4` (`verify_limit_multiplier=4`).

## ⚙️ Hardcoded Defaults and Assumptions

- `--request-delay` is global for all fetches (search, source scan, verification).
- Verification is heuristic:
  - only HTTP `200` pages can be `VERIFIED`
  - score threshold is `positive >= 3` and `negative <= 2`
- If the query file is missing or empty, discovery uses in-code fallback queries.
- Domain seeds are expanded to fixed paths:
  - `/careers`, `/jobs`, `/company/careers`, `/about/careers`
- Retry wait and cache TTL are fixed:
  - retry backoff: `1.5 * attempt`
  - cache TTL: `24h`

## 📚 Documentation

- Architecture and flow: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- CLI flags and defaults: [docs/CLI_REFERENCE.md](docs/CLI_REFERENCE.md)
- SQLite schema and table purpose: [docs/SQLITE_SCHEMA.md](docs/SQLITE_SCHEMA.md)
- Logs, progress bars, troubleshooting: [docs/OPERATIONS.md](docs/OPERATIONS.md)

## 🛠️ Testing and Maintenance

Run tests:

```bash
pytest -q
```

Quick seed-only smoke check:

```bash
python -m ashby_discovery \
  --input-company-list seed_list.txt \
  --output-dir output \
  --search-pages-per-query 0
```
