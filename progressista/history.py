"""SQLite-backed persistent history of completed runs.

The live task snapshot in `server.py` covers in-flight recovery (a single
JSON file rewritten on every progress tick). It is intentionally lossy:
once a task closes and ages past `retention_seconds`, it disappears.

This module owns the long-term record. One row per terminal transition
(close, error, failed, or stale-evicted by GC). Writes are single-row
INSERTs in their own transaction - no full-snapshot rewrite race.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger("progressista.history")

TERMINAL_STATUSES = ("close", "error", "failed", "stale-evicted")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    parent_id   TEXT,
    desc        TEXT,
    unit        TEXT,
    total       REAL,
    n_final     REAL,
    status      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    done_at     REAL NOT NULL,
    duration_s  REAL,
    meta_json   TEXT
);
CREATE INDEX IF NOT EXISTS runs_task_done ON runs(task_id, done_at DESC);
CREATE INDEX IF NOT EXISTS runs_done      ON runs(done_at DESC);
"""


class HistoryStore:
    """Async-friendly wrapper around a single SQLite file.

    Connections are opened per call (cheap for sqlite) so we sidestep
    Python's per-connection thread-affinity. Writes still serialize via
    `_write_lock` to keep busy-timeouts from piling up under bursts.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = asyncio.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None -> autocommit; explicit txns where needed.
        # timeout > 0 lets concurrent readers wait on a writer briefly.
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=5.0)
        conn.row_factory = sqlite3.Row
        # WAL improves concurrent read-while-write; cheap for our volumes.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_schema(self) -> None:
        try:
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
        except Exception:
            LOGGER.exception("Failed to initialise history db at %s", self.db_path)
            raise

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        out = dict(row)
        meta_json = out.pop("meta_json", None)
        if meta_json:
            try:
                out["meta"] = json.loads(meta_json)
            except Exception:
                out["meta"] = None
        else:
            out["meta"] = None
        return out

    @staticmethod
    def _extract_parent_id(task: Dict[str, Any]) -> Optional[str]:
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else None
        if not meta:
            return None
        pid = meta.get("parent_id")
        return pid if isinstance(pid, str) else None

    async def record(self, task: Dict[str, Any], status: str, done_at: float) -> None:
        """Record a terminal transition for `task`.

        Caller decides the canonical status (`close`, `error`, `failed`,
        `stale-evicted`) - we just persist whatever arrives.
        """

        async with self._write_lock:
            await asyncio.get_running_loop().run_in_executor(
                None, self._record_sync, task, status, done_at
            )

    def _record_sync(self, task: Dict[str, Any], status: str, done_at: float) -> None:
        created_at = task.get("created_at") or done_at
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else None
        meta_json = json.dumps(meta, ensure_ascii=True) if meta else None
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO runs (
                        task_id, parent_id, desc, unit, total, n_final,
                        status, created_at, done_at, duration_s, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task.get("task_id"),
                        self._extract_parent_id(task),
                        task.get("desc"),
                        task.get("unit"),
                        task.get("total"),
                        task.get("n"),
                        status,
                        created_at,
                        done_at,
                        max(0.0, done_at - created_at),
                        meta_json,
                    ),
                )
        except Exception:
            # Don't let history failures break live progress reporting.
            LOGGER.exception("Failed to record history row for task %s", task.get("task_id"))

    async def query(
        self,
        limit: int = 50,
        before_id: Optional[int] = None,
        task_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await asyncio.get_running_loop().run_in_executor(
            None, self._query_sync, limit, before_id, task_id
        )

    def _query_sync(
        self,
        limit: int,
        before_id: Optional[int],
        task_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        clauses: List[str] = []
        params: List[Any] = []
        if before_id is not None:
            clauses.append("id < ?")
            params.append(int(before_id))
        if task_id:
            clauses.append("task_id LIKE ?")
            params.append(task_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM runs {where} ORDER BY id DESC LIMIT ?"
        params.append(limit)
        try:
            with self._connect() as conn:
                rows = conn.execute(sql, params).fetchall()
            return [self._row_to_dict(r) for r in rows]
        except Exception:
            LOGGER.exception("Failed to query history")
            return []

    async def get(self, history_id: int) -> Optional[Dict[str, Any]]:
        return await asyncio.get_running_loop().run_in_executor(
            None, self._get_sync, history_id
        )

    def _get_sync(self, history_id: int) -> Optional[Dict[str, Any]]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM runs WHERE id = ?", (int(history_id),)
                ).fetchone()
            return self._row_to_dict(row) if row else None
        except Exception:
            LOGGER.exception("Failed to fetch history row %s", history_id)
            return None

    async def delete(self, history_id: int) -> bool:
        async with self._write_lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._delete_sync, history_id
            )

    def _delete_sync(self, history_id: int) -> bool:
        try:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM runs WHERE id = ?", (int(history_id),))
                return cur.rowcount > 0
        except Exception:
            LOGGER.exception("Failed to delete history row %s", history_id)
            return False

    async def stats(self) -> Dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(None, self._stats_sync)

    def _stats_sync(self) -> Dict[str, Any]:
        try:
            with self._connect() as conn:
                total = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                by_status_rows = conn.execute(
                    "SELECT status, COUNT(*) FROM runs GROUP BY status"
                ).fetchall()
            return {
                "total": total,
                "by_status": {r[0]: r[1] for r in by_status_rows},
            }
        except Exception:
            LOGGER.exception("Failed to read history stats")
            return {"total": 0, "by_status": {}}


__all__ = ["HistoryStore", "TERMINAL_STATUSES"]
