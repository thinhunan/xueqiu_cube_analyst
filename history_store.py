"""调仓历史 SQLite 本地缓存。"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

DB_PATH = Path(__file__).resolve().parent / "data" / "rebalancing_history.db"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS rebalancing_records (
                cube_symbol TEXT NOT NULL,
                record_id   INTEGER NOT NULL,
                created_at  INTEGER,
                payload     TEXT NOT NULL,
                PRIMARY KEY (cube_symbol, record_id)
            );
            CREATE INDEX IF NOT EXISTS idx_rebalancing_symbol_created
                ON rebalancing_records (cube_symbol, created_at DESC);

            CREATE TABLE IF NOT EXISTS rebalancing_meta (
                cube_symbol TEXT PRIMARY KEY,
                total_count INTEGER NOT NULL DEFAULT 0,
                updated_at  TEXT NOT NULL
            );
            """
        )


def has_records(cube_symbol: str) -> bool:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM rebalancing_records WHERE cube_symbol = ? LIMIT 1",
            (cube_symbol,),
        ).fetchone()
        return row is not None


def get_existing_ids(cube_symbol: str) -> Set[int]:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT record_id FROM rebalancing_records WHERE cube_symbol = ?",
            (cube_symbol,),
        ).fetchall()
    return {int(row["record_id"]) for row in rows}


def get_record_count(cube_symbol: str) -> int:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM rebalancing_records WHERE cube_symbol = ?",
            (cube_symbol,),
        ).fetchone()
    return int(row["cnt"]) if row else 0


def load_records(cube_symbol: str) -> List[Dict[str, Any]]:
    """按 created_at 降序返回（与 API list 顺序一致，最新在前）。"""
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT payload FROM rebalancing_records
            WHERE cube_symbol = ?
            ORDER BY created_at DESC, record_id DESC
            """,
            (cube_symbol,),
        ).fetchall()
    return [json.loads(row["payload"]) for row in rows]


def get_total_count(cube_symbol: str) -> int:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT total_count FROM rebalancing_meta WHERE cube_symbol = ?",
            (cube_symbol,),
        ).fetchone()
    if row:
        return int(row["total_count"])
    return get_record_count(cube_symbol)


def save_records(cube_symbol: str, records: List[Dict[str, Any]], total_count: int) -> int:
    """写入新记录，返回本次新增条数。"""
    if not records:
        update_meta(cube_symbol, total_count)
        return 0

    init_db()
    inserted = 0
    with _connect() as conn:
        for rec in records:
            record_id = rec.get("id")
            if record_id is None:
                continue
            created_at = rec.get("created_at")
            try:
                created_at = int(created_at) if created_at is not None else 0
            except (TypeError, ValueError):
                created_at = 0
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO rebalancing_records
                    (cube_symbol, record_id, created_at, payload)
                VALUES (?, ?, ?, ?)
                """,
                (cube_symbol, int(record_id), created_at, json.dumps(rec, ensure_ascii=False)),
            )
            inserted += cur.rowcount
        conn.execute(
            """
            INSERT INTO rebalancing_meta (cube_symbol, total_count, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(cube_symbol) DO UPDATE SET
                total_count = excluded.total_count,
                updated_at = excluded.updated_at
            """,
            (cube_symbol, int(total_count), _utc_now_iso()),
        )
    return inserted


def update_meta(cube_symbol: str, total_count: int) -> None:
    init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO rebalancing_meta (cube_symbol, total_count, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(cube_symbol) DO UPDATE SET
                total_count = excluded.total_count,
                updated_at = excluded.updated_at
            """,
            (cube_symbol, int(total_count), _utc_now_iso()),
        )


def build_history_result(cube_symbol: str, total_count: Optional[int] = None) -> Dict[str, Any]:
    records = load_records(cube_symbol)
    if total_count is None:
        total_count = get_total_count(cube_symbol)
    total_count = max(int(total_count), len(records))
    return {
        "totalCount": total_count,
        "list": records,
        "fetchedCount": len(records),
        "fromCache": True,
    }
