# 🧭 CLI Reference

Command:

```bash
python -m ashby_discovery [options]
```

## ⚙️ Options

| Option | Default | Description |
|---|---|---|
| `--max-results` | `500` | Main run budget. Export is capped at this value. Search URL budget is `max_results * 4`. Verification budget is `max_results * 4` (`verify_limit_multiplier=4`). |
| `--input-company-list` | _none_ | Text file of domains/URLs to seed discovery. |
| `--output-dir` | `output` | Directory for output files and default DB path. |
| `--db-path` | _none_ | Explicit SQLite path (overrides default `output-dir` DB path). |
| `--resume` | `false` | Resume using existing SQLite state. |
| `--search-pages-per-query` | `5` | Number of paginated search pages per query/provider. Set `0` to skip search discovery. |
| `--search-engines` | `duckduckgo,brave,yahoo` | Comma-separated providers in priority order. |
| `--search-queries-file` | _none_ | Query file path (one query per line). If missing, uses repo `search_queries.txt` if available. |
| `--source-scan-batch-size` | `25` | Number of source pages scanned per batch. |
| `--timeout` | `15.0` | HTTP timeout in seconds. |
| `--retries` | `3` | Retry attempts for HTTP requests. |
| `--request-delay` | `0.25` | Minimum delay between requests (seconds). |
| `--max-connections` | `20` | Maximum concurrent HTTP connections. |
| `--user-agent` | Chrome-like UA | User-Agent header for requests. |
| `--include-unverified` | `false` | Include `NOT_VERIFIED`/`ERROR` rows in output. |
| `--verbose` | `false` | Enable detailed phase/debug logs. |
| `--no-progress` | `false` | Disable tqdm progress bars. |

## 🧪 Examples

Basic:

```bash
python -m ashby_discovery --max-results 500 --output-dir output
```

Resume and verbose:

```bash
python -m ashby_discovery --output-dir output --resume --verbose
```

Seed-only mode:

```bash
python -m ashby_discovery \
  --input-company-list seed_list.txt \
  --output-dir output \
  --resume \
  --search-pages-per-query 0
```

## 📌 Hardcoded Defaults and Assumptions

These defaults are not exposed as CLI flags:

- Verification limit multiplier: `4` (`verify_limit = max_results * 4`).
- Fetch-cache TTL: `24h` (`86400s`).
- Retry backoff wait: `1.5 * attempt` seconds.
- `ddg` is accepted as alias for `duckduckgo`.
- Unsupported values in `--search-engines` are ignored. If all are invalid, defaults are used (`duckduckgo,brave,yahoo`).
- Domain seeds are expanded to fixed paths: `/careers`, `/jobs`, `/company/careers`, `/about/careers`.
