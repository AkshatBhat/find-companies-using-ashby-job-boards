# 🛠️ Operations Guide

## 🧾 Verbose Output (`--verbose`)

Verbose mode prints timestamped stage logs while the pipeline runs.

Example (search-driven run):

```text
[03:58:32] Opening discovery store at output/discovery_state.sqlite
[03:58:32] SQLite index health: {'sources_queue': True, 'unverified_slugs': True, 'verified_export_order': True}
[03:58:32] Starting candidate discovery
[03:58:32] Starting search discovery with engines=duckduckgo,brave,yahoo queries=5
[03:58:32] [search] running query: site:jobs.ashbyhq.com
[03:58:36] Search discovery produced 64 candidate URLs
[03:58:45] Starting verification for 120 unverified slugs
[03:58:58] Exporting 46 cumulative rows to CSV/JSON (this run: +12)
[03:58:58] Recorded 78 cumulative failures (this run: +5)
```

Example (seed-only mode):

```text
[03:58:32] Starting candidate discovery
[03:58:32] Search discovery skipped because --search-pages-per-query=0
[03:58:32] Loaded 352 company seeds from input file
```

## 🏷️ Prefix Reference

- No prefix: high-level phase transitions.
- `[search]`: search query/provider progress, backoff, and cooldown/disable events.
- `[source_scan]`: per-source-page scan result (success or failure).
- `Verified X/Y slugs so far`: verification progress checkpoints.

## 📊 Progress Bars

- `Search pages`: progress across query/provider page fetches.
- `Source pages`: progress across queued source-page scans.
- `Verifying slugs`: progress across slug verification calls.

If your terminal does not render progress bars well, use `--no-progress` for clean static logs.

## 🧊 Search Provider Backoff Policy

Search discovery applies provider-level cooldown when a provider returns challenge/rate-limit pages.

- Cooldown starts at `20s` and doubles on repeated blocks (`20s -> 40s -> 80s ...`), capped at `300s`.
- A provider is disabled for the current run after `10` blocked events.
- A successful result page resets penalty/cooldown state for that provider.

## 📌 Hardcoded Runtime Assumptions

- Request pacing is global: `--request-delay` applies between all fetches (search, source scan, and verification).
- Cache usage is stage-dependent:
  - Search discovery bypasses cache (`use_cache=False`).
  - Verification bypasses cache (`use_cache=False`).
  - Source scanning uses cache by default (`use_cache=True`).
- Search block detection treats `429` as `rate_limited`, `403` as `challenge`, and also checks challenge markers in HTML.
- Verification marks a slug `NOT_VERIFIED` for any non-`200` response.
- Failure messages are truncated to `4000` chars before SQLite/JSONL persistence.

## 🔁 Resume Mode Counters

With `--resume`, summary lines show cumulative totals and current run deltas:

- `Rows exported (cumulative): X (this run: Y)`
- `Failures recorded (cumulative): X (this run: Y)`

## 🧯 Troubleshooting

### 🚫 1) No new candidates

Symptoms:

- `Candidates added: 0`
- `Source pages scanned: 0`

Possible causes:

- Search providers returned blocks/challenges.
- Resume DB already contains previously discovered data.

Actions:

- Check `failures.jsonl` for `search_discovery:*` errors.
- Try `--resume` with different engine order.
- Increase/decrease `--search-pages-per-query` depending on provider behavior.
- Provide explicit seeds via `--input-company-list`.

### 🌱 2) Seed-only run expected

Use:

```bash
python -m ashby_discovery \
  --input-company-list seed_list.txt \
  --output-dir output \
  --resume \
  --search-pages-per-query 0
```

### 👀 3) Progress bars not visible

- Ensure dependencies are installed (`pip install -r requirements.txt`).
- Disable bars with `--no-progress` if terminal handling is noisy.
