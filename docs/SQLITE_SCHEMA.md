# 🗄️ SQLite Schema

The pipeline stores state in SQLite (`discovery_state.sqlite`) for deduplication, caching, and resume support.

## 📥 `sources`

Purpose: queue and track pages to scan for Ashby embed markers.

| Column | Type | Notes |
|---|---|---|
| `url` | `TEXT` | Primary key. |
| `source_type` | `TEXT` | Origin of source page (`search_result_page`, `input_company_list`, etc.). |
| `scanned` | `INTEGER` | `0`/`1` status. |
| `last_error` | `TEXT` | Last scan error, if any. |
| `updated_at` | `TEXT` | ISO timestamp. |

Why needed:

- Prevents rescanning already processed pages.
- Supports resume.
- Keeps scan failure context.

## 🧩 `candidates`

Purpose: store discovered slug candidates with source context.

| Column | Type | Notes |
|---|---|---|
| `slug` | `TEXT` | Candidate slug. |
| `source_type` | `TEXT` | Discovery channel (`direct_search`, `embedded_careers_page`, `other`). |
| `source_url` | `TEXT` | URL where candidate was found. |
| `notes` | `TEXT` | Additional context. |
| `discovered_at` | `TEXT` | ISO timestamp. |

Constraints:

- Unique composite key: `(slug, source_type, source_url)`.

Why needed:

- Deduplicates repeated findings.
- Keeps source context for auditing.

## ✅ `verifications`

Purpose: store slug verification status and canonical Ashby URL.

| Column | Type | Notes |
|---|---|---|
| `slug` | `TEXT` | Primary key. |
| `ashby_url` | `TEXT` | Canonical board URL used for validation. |
| `verification_status` | `TEXT` | `VERIFIED`, `NOT_VERIFIED`, or `ERROR`. |
| `inferred_company_name` | `TEXT` | Optional enrichment. |
| `notes` | `TEXT` | Verification notes/score context. |
| `http_status` | `INTEGER` | Response status if request completed. |
| `checked_at` | `TEXT` | ISO timestamp. |

Why needed:

- Prevents redundant re-verification.
- Stores final outcome for export.

## 💾 `fetch_cache`

Purpose: cache fetched pages to reduce repeated requests.

| Column | Type | Notes |
|---|---|---|
| `url` | `TEXT` | Primary key request URL. |
| `final_url` | `TEXT` | Resolved URL after redirects. |
| `status_code` | `INTEGER` | HTTP status. |
| `text` | `TEXT` | Response body. |
| `fetched_at` | `TEXT` | ISO timestamp. |

Why needed:

- Reduces provider load.
- Speeds up reruns.

## 🚨 `failures`

Purpose: failure audit log by pipeline stage.

| Column | Type | Notes |
|---|---|---|
| `id` | `INTEGER` | Auto-increment primary key. |
| `stage` | `TEXT` | Pipeline stage (`search_discovery:*`, `source_scan`, `verify_slug`, etc.). |
| `target` | `TEXT` | URL/query/slug that failed. |
| `error` | `TEXT` | Truncated error message. |
| `created_at` | `TEXT` | ISO timestamp. |

Why needed:

- Supports debugging and operational visibility.
- Exported as `failures.jsonl`.

## 🚀 Performance Indexes

The schema also creates indexes for frequent query paths:

- `idx_sources_scanned_updated_at` on `sources(scanned, updated_at)`
- `idx_candidates_discovered_slug` on `candidates(discovered_at, slug)`
- `idx_candidates_slug_discovered` on `candidates(slug, discovered_at)`
- `idx_verifications_status_slug` on `verifications(verification_status, slug)`

These do not change results. They improve queue/export query speed on large DBs.
