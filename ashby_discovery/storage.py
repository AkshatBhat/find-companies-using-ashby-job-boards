from __future__ import annotations

from contextlib import contextmanager
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional

from .models import CandidateSlug, FetchedPage, VerificationResult


class DiscoveryStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._batch_depth = 0
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS sources (
                url TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                scanned INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS candidates (
                slug TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT NOT NULL,
                notes TEXT,
                discovered_at TEXT NOT NULL,
                UNIQUE(slug, source_type, source_url)
            );

            CREATE TABLE IF NOT EXISTS verifications (
                slug TEXT PRIMARY KEY,
                ashby_url TEXT NOT NULL,
                verification_status TEXT NOT NULL,
                inferred_company_name TEXT,
                notes TEXT,
                http_status INTEGER,
                checked_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fetch_cache (
                url TEXT PRIMARY KEY,
                final_url TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                text TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage TEXT NOT NULL,
                target TEXT NOT NULL,
                error TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sources_scanned_updated_at
                ON sources(scanned, updated_at);
            CREATE INDEX IF NOT EXISTS idx_candidates_discovered_slug
                ON candidates(discovered_at, slug);
            CREATE INDEX IF NOT EXISTS idx_candidates_slug_discovered
                ON candidates(slug, discovered_at);
            CREATE INDEX IF NOT EXISTS idx_verifications_status_slug
                ON verifications(verification_status, slug);
            """
        )
        self.conn.commit()

    @contextmanager
    def batch_write(self) -> Iterator[None]:
        self._batch_depth += 1
        try:
            yield
        except Exception:
            if self._batch_depth == 1:
                self.conn.rollback()
            raise
        else:
            if self._batch_depth == 1:
                self.conn.commit()
        finally:
            self._batch_depth -= 1

    def _maybe_commit(self) -> None:
        if self._batch_depth == 0:
            self.conn.commit()

    def upsert_source(self, url: str, source_type: str) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """
            INSERT INTO sources(url, source_type, scanned, updated_at)
            VALUES (?, ?, 0, ?)
            ON CONFLICT(url) DO UPDATE SET
                source_type = excluded.source_type,
                updated_at = excluded.updated_at
            """,
            (url, source_type, now),
        )
        self._maybe_commit()

    def mark_source_scanned(self, url: str, error: Optional[str] = None) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """
            UPDATE sources
            SET scanned = 1,
                last_error = ?,
                updated_at = ?
            WHERE url = ?
            """,
            (error, now, url),
        )
        self._maybe_commit()

    def get_unscanned_sources(self, limit: int) -> list[tuple[str, str]]:
        rows = self.conn.execute(
            """
            SELECT url, source_type
            FROM sources
            WHERE scanned = 0
            ORDER BY updated_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [(r["url"], r["source_type"]) for r in rows]

    def count_unscanned_sources(self) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM sources
            WHERE scanned = 0
            """
        ).fetchone()
        return int(row["n"])

    def add_candidates(self, candidates: Iterable[CandidateSlug]) -> int:
        now = datetime.utcnow().isoformat()
        rows = [
            (c.slug, c.source_type, c.source_url, c.notes, c.discovered_at.isoformat() if c.discovered_at else now)
            for c in candidates
        ]
        if not rows:
            return 0
        cur = self.conn.executemany(
            """
            INSERT OR IGNORE INTO candidates(slug, source_type, source_url, notes, discovered_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._maybe_commit()
        return cur.rowcount if cur.rowcount != -1 else 0

    def list_unverified_slugs(self, limit: int) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT c.slug
            FROM candidates c
            LEFT JOIN verifications v ON v.slug = c.slug
            WHERE v.slug IS NULL
            ORDER BY c.discovered_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [r["slug"] for r in rows]

    def get_sources_for_slug(self, slug: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT source_type, source_url, notes
            FROM candidates
            WHERE slug = ?
            ORDER BY discovered_at ASC
            """,
            (slug,),
        ).fetchall()

    def save_verification(self, result: VerificationResult) -> None:
        self.conn.execute(
            """
            INSERT INTO verifications(
                slug,
                ashby_url,
                verification_status,
                inferred_company_name,
                notes,
                http_status,
                checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                ashby_url = excluded.ashby_url,
                verification_status = excluded.verification_status,
                inferred_company_name = excluded.inferred_company_name,
                notes = excluded.notes,
                http_status = excluded.http_status,
                checked_at = excluded.checked_at
            """,
            (
                result.slug,
                result.ashby_url,
                result.verification_status,
                result.inferred_company_name,
                result.notes,
                result.http_status,
                result.checked_at.isoformat(),
            ),
        )
        self._maybe_commit()

    def load_cached_fetch(self, url: str, ttl_seconds: int) -> Optional[FetchedPage]:
        row = self.conn.execute(
            """
            SELECT url, final_url, status_code, text, fetched_at
            FROM fetch_cache
            WHERE url = ?
            """,
            (url,),
        ).fetchone()
        if row is None:
            return None
        fetched_at = datetime.fromisoformat(row["fetched_at"])
        if datetime.utcnow() - fetched_at > timedelta(seconds=ttl_seconds):
            return None
        return FetchedPage(
            url=row["url"],
            final_url=row["final_url"],
            status_code=row["status_code"],
            text=row["text"],
            fetched_at=fetched_at,
        )

    def save_cached_fetch(self, page: FetchedPage) -> None:
        self.conn.execute(
            """
            INSERT INTO fetch_cache(url, final_url, status_code, text, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                final_url = excluded.final_url,
                status_code = excluded.status_code,
                text = excluded.text,
                fetched_at = excluded.fetched_at
            """,
            (
                page.url,
                page.final_url,
                page.status_code,
                page.text,
                page.fetched_at.isoformat(),
            ),
        )
        self._maybe_commit()

    def record_failure(self, stage: str, target: str, error: str) -> None:
        self.conn.execute(
            """
            INSERT INTO failures(stage, target, error, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (stage, target, error[:4000], datetime.utcnow().isoformat()),
        )
        self._maybe_commit()

    def record_failures(self, failures: Iterable[tuple[str, str, str]]) -> int:
        rows = [(stage, target, error[:4000], datetime.utcnow().isoformat()) for stage, target, error in failures]
        if not rows:
            return 0
        cur = self.conn.executemany(
            """
            INSERT INTO failures(stage, target, error, created_at)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        self._maybe_commit()
        return cur.rowcount if cur.rowcount != -1 else 0

    def save_verifications(self, results: Iterable[VerificationResult]) -> int:
        rows = [
            (
                result.slug,
                result.ashby_url,
                result.verification_status,
                result.inferred_company_name,
                result.notes,
                result.http_status,
                result.checked_at.isoformat(),
            )
            for result in results
        ]
        if not rows:
            return 0
        cur = self.conn.executemany(
            """
            INSERT INTO verifications(
                slug,
                ashby_url,
                verification_status,
                inferred_company_name,
                notes,
                http_status,
                checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                ashby_url = excluded.ashby_url,
                verification_status = excluded.verification_status,
                inferred_company_name = excluded.inferred_company_name,
                notes = excluded.notes,
                http_status = excluded.http_status,
                checked_at = excluded.checked_at
            """,
            rows,
        )
        self._maybe_commit()
        return cur.rowcount if cur.rowcount != -1 else 0

    def count_export_results(self, verified_only: bool = True) -> int:
        if verified_only:
            row = self.conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM verifications
                WHERE verification_status = 'VERIFIED'
                """
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM verifications
                """
            ).fetchone()
        return int(row["n"])

    def index_health(self) -> dict[str, bool]:
        checks: dict[str, tuple[str, tuple]] = {
            "sources_queue": (
                "SELECT url, source_type FROM sources WHERE scanned = 0 ORDER BY updated_at ASC LIMIT 10",
                (),
            ),
            "unverified_slugs": (
                (
                    "SELECT DISTINCT c.slug FROM candidates c "
                    "LEFT JOIN verifications v ON v.slug = c.slug "
                    "WHERE v.slug IS NULL ORDER BY c.discovered_at ASC LIMIT 10"
                ),
                (),
            ),
            "verified_export_order": (
                "SELECT slug FROM verifications WHERE verification_status = 'VERIFIED' ORDER BY slug ASC LIMIT 10",
                (),
            ),
        }

        out: dict[str, bool] = {}
        for key, (query, params) in checks.items():
            rows = self.conn.execute(f"EXPLAIN QUERY PLAN {query}", params).fetchall()
            details = " | ".join(str(row["detail"]).upper() for row in rows)
            out[key] = "USING INDEX" in details or "USING COVERING INDEX" in details
        return out

    def export_results(self, verified_only: bool = True) -> list[dict]:
        condition = "WHERE v.verification_status = 'VERIFIED'" if verified_only else ""
        rows = self.conn.execute(
            f"""
            SELECT
                v.slug,
                COALESCE(v.inferred_company_name, '') AS inferred_company_name,
                v.ashby_url,
                c.source_type,
                c.source_url,
                v.verification_status,
                COALESCE(v.notes, '') AS notes
            FROM verifications v
            LEFT JOIN candidates c ON c.slug = v.slug
            {condition}
            GROUP BY v.slug
            ORDER BY v.slug ASC
            """
        ).fetchall()

        return [
            {
                "slug": row["slug"],
                "inferred_company_name": row["inferred_company_name"],
                "ashby_url": row["ashby_url"],
                "source_type": row["source_type"],
                "source_url": row["source_url"],
                "verification_status": row["verification_status"],
                "notes": row["notes"],
            }
            for row in rows
        ]

    def export_failures(self) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT stage, target, error, created_at
            FROM failures
            ORDER BY id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for name in ("sources", "candidates", "verifications", "failures"):
            row = self.conn.execute(f"SELECT COUNT(*) AS n FROM {name}").fetchone()
            out[name] = int(row["n"])
        return out
