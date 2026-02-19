"""
SQLite glossary DB utilities.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path


def get_db_path() -> str:
    env_path = os.environ.get("GLOSSARY_DB_PATH")
    if env_path:
        return env_path
    return str(Path(__file__).parent / "glossary.db")


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    _ensure_parent_dir(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | None = None) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS glossary_terms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                english TEXT NOT NULL,
                korean TEXT NOT NULL,
                transliteration TEXT NOT NULL DEFAULT '',
                context_note TEXT NOT NULL DEFAULT '',
                source_paper TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (english, korean, transliteration, source_paper)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_glossary_source ON glossary_terms(source_paper)"
        )
        conn.commit()


def _normalize_term(term: dict) -> dict:
    return {
        "english": (term.get("english") or "").strip(),
        "korean": (term.get("korean") or "").strip(),
        "transliteration": (term.get("transliteration") or "").strip(),
        "context_note": (term.get("context_note") or "").strip(),
    }


def upsert_terms(
    db_path: str | None,
    terms: list[dict],
    source_paper: str | None = None,
) -> int:
    if not terms:
        return 0
    source = (source_paper or "").strip()
    now = datetime.now().isoformat()
    rows = []
    for t in terms:
        nt = _normalize_term(t)
        if not nt["english"] or not nt["korean"]:
            continue
        rows.append(
            (
                nt["english"],
                nt["korean"],
                nt["transliteration"],
                nt["context_note"],
                source,
                now,
                now,
            )
        )

    if not rows:
        return 0

    with connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO glossary_terms (
                english, korean, transliteration, context_note,
                source_paper, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(english, korean, transliteration, source_paper) DO UPDATE SET
                context_note = CASE
                    WHEN excluded.context_note <> ''
                    THEN excluded.context_note
                    ELSE glossary_terms.context_note
                END,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def fetch_terms_for_paper(
    db_path: str | None, source_paper: str
) -> list[dict]:
    source = (source_paper or "").strip()
    if not source:
        return []
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            SELECT english, korean, transliteration, context_note
            FROM glossary_terms
            WHERE source_paper = ?
            ORDER BY english COLLATE NOCASE
            """,
            (source,),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_terms(
    db_path: str | None, limit: int | None = None
) -> list[dict]:
    sql = """
        SELECT english, korean, transliteration, context_note, source_paper
        FROM glossary_terms
        ORDER BY english COLLATE NOCASE
    """
    params: tuple = ()
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params = (limit,)
    with connect(db_path) as conn:
        cur = conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
