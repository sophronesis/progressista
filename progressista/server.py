"""FastAPI application that collects progress updates and pushes them to browsers."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from importlib import resources
from pathlib import Path
from typing import Any, Dict, Set

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import __version__
from .history import HistoryStore, TERMINAL_STATUSES
from .settings import ServerSettings

LOGGER = logging.getLogger("progressista.server")


class ProgressEvent(BaseModel):
    """Payload received from clients to describe task progress."""

    task_id: str = Field(..., description="Unique identifier for this task.")
    desc: str | None = Field(None, description="Human readable description.")
    total: float | None = Field(None, description="Total units of work.")
    n: float | None = Field(None, description="Completed units of work.")
    unit: str | None = Field(None, description="Display unit for the work done.")
    status: str | None = Field(
        None, description="Optional status string such as start, update, close, error."
    )
    timestamp: float | None = Field(None, description="Unix epoch seconds for the event.")
    meta: Dict[str, Any] | None = Field(
        default=None,
        description="Optional free-form metadata provided by the client.",
    )


def _extract_bearer(header: str | None) -> str | None:
    if not header:
        return None
    header = header.strip()
    if not header.lower().startswith("bearer "):
        return None
    return header[7:].strip() or None


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""

    settings = settings or ServerSettings()
    app = FastAPI(title="Progressista", version=__version__)
    static_dir = resources.files("progressista") / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    storage_path: Path | None = None
    if settings.storage_path:
        candidate = Path(settings.storage_path).expanduser()
        try:
            candidate.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            LOGGER.exception("Failed to prepare storage directory for %s", candidate)
        else:
            storage_path = candidate

    # History DB defaults to a sibling of storage_path when only one is set.
    history_db_path: Path | None = None
    raw_history = settings.history_db_path
    if raw_history:
        history_db_path = Path(raw_history).expanduser()
    elif storage_path is not None:
        history_db_path = storage_path.parent / "history.db"

    history: HistoryStore | None = None
    if history_db_path is not None:
        try:
            history = HistoryStore(history_db_path)
            LOGGER.info("History DB ready at %s", history_db_path)
        except Exception:
            LOGGER.exception("Failed to open history DB at %s", history_db_path)
            history = None

    # Mutable state kept on the app object.
    app.state.tasks: Dict[str, Dict[str, Any]] = {}
    app.state.state_lock = asyncio.Lock()
    app.state.watchers: Set[WebSocket] = set()
    app.state.watchers_lock = asyncio.Lock()
    app.state.settings = settings
    app.state.cleanup_task: asyncio.Task[None] | None = None
    app.state.storage_path = storage_path
    app.state.persist_lock = asyncio.Lock()
    app.state.history = history

    def load_persisted_tasks(path: Path | None) -> Dict[str, Dict[str, Any]]:
        if not path or not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as source:
                payload = json.load(source)
        except Exception:
            LOGGER.exception("Failed to load persisted tasks from %s", path)
            return {}
        tasks = payload.get("tasks")
        if not isinstance(tasks, dict):
            return {}
        now = time.time()
        restored: Dict[str, Dict[str, Any]] = {}
        for task_id, raw in tasks.items():
            if not isinstance(task_id, str) or not isinstance(raw, dict):
                continue
            task = dict(raw)
            task["task_id"] = task.get("task_id", task_id)
            task.setdefault("created_at", now)
            task.setdefault("updated_at", task.get("created_at", now))
            status = str(task.get("status", "recovered") or "recovered")
            if status == "close":
                task["status"] = "close"
            elif status == "stale":
                task["status"] = "stale"
            else:
                task["status"] = "recovered"
            task["recovered"] = True
            task["recovered_at"] = now
            restored[task_id] = task
        return restored

    async def persist_state(snapshot: Dict[str, Dict[str, Any]]) -> None:
        path: Path | None = app.state.storage_path
        if not path:
            return
        data = {
            "tasks": snapshot,
            "version": __version__,
            "saved_at": time.time(),
        }
        json_payload = json.dumps(data, ensure_ascii=True, separators=(",", ":"))
        async with app.state.persist_lock:
            loop = asyncio.get_running_loop()

            def write_snapshot() -> None:
                tmp_path = path.with_suffix(".tmp")
                with tmp_path.open("w", encoding="utf-8") as target:
                    target.write(json_payload)
                tmp_path.replace(path)

            try:
                await loop.run_in_executor(None, write_snapshot)
            except Exception:
                LOGGER.exception("Failed to persist tasks to %s", path)

    if storage_path:
        persisted = load_persisted_tasks(storage_path)
        if persisted:
            app.state.tasks.update(persisted)

    if settings.allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.allow_origins),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    async def get_snapshot() -> Dict[str, Dict[str, Any]]:
        async with app.state.state_lock:
            return {k: dict(v) for k, v in app.state.tasks.items()}

    def require_token(*candidates: str | None) -> None:
        tokens = app.state.settings.api_tokens
        if not tokens:
            return
        for candidate in candidates:
            if candidate and candidate in tokens:
                return
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API token",
        )

    def extract_request_tokens(request: Request) -> tuple[str | None, str | None]:
        query_token = request.query_params.get("token")
        header_token = _extract_bearer(request.headers.get("authorization"))
        return query_token, header_token

    async def cleanup_loop() -> None:
        LOGGER.info(
            "Starting cleanup loop (interval=%s, retention=%s)",
            settings.cleanup_interval,
            settings.retention_seconds,
        )
        try:
            while True:
                await asyncio.sleep(settings.cleanup_interval)
                now = time.time()
                updated = False
                evicted_for_history: list[Dict[str, Any]] = []
                async with app.state.state_lock:
                    stale_ids = [
                        task_id
                        for task_id, data in list(app.state.tasks.items())
                        if data.get("status") == "close"
                        and settings.retention_seconds > 0
                        and now - data.get("updated_at", data.get("created_at", now))
                        > settings.retention_seconds
                    ]

                    removed_ids = list(stale_ids)
                    stale_changed = False

                    if settings.max_task_age > 0:
                        for task_id, data in list(app.state.tasks.items()):
                            updated_at = data.get("updated_at", data.get("created_at", now))
                            if now - updated_at > settings.max_task_age:
                                removed_ids.append(task_id)

                    if settings.stale_seconds > 0:
                        for task_id, data in list(app.state.tasks.items()):
                            status_val = data.get("status")
                            updated_at = data.get("updated_at", data.get("created_at", now))
                            if (
                                status_val not in ("close", "stale")
                                and now - updated_at > settings.stale_seconds
                            ):
                                data["status"] = "stale"
                                data.setdefault("stale_at", now)
                                app.state.tasks[task_id] = data
                                stale_changed = True

                    for task_id in removed_ids:
                        data = app.state.tasks.pop(task_id, None)
                        if (
                            app.state.history is not None
                            and data is not None
                            and data.get("status") not in TERMINAL_STATUSES
                        ):
                            # Open task being purged by GC - preserve a row
                            # so it doesn't vanish silently from history.
                            evicted_for_history.append(dict(data))

                    updated = bool(removed_ids) or stale_changed

                for data in evicted_for_history:
                    await app.state.history.record(data, "stale-evicted", now)
                if updated:
                    snapshot = await get_snapshot()
                    await persist_state(snapshot)
                    await broadcast(snapshot)
        except asyncio.CancelledError:  # pragma: no cover - clean shutdown
            LOGGER.info("Cleanup loop cancelled.")
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Cleanup loop crashed.")

    async def broadcast(snapshot: Dict[str, Dict[str, Any]]) -> None:
        if not snapshot:
            payload = {"tasks": {}}
        else:
            payload = {"tasks": snapshot}

        async with app.state.watchers_lock:
            if not app.state.watchers:
                return
            watchers = list(app.state.watchers)

        dead: list[WebSocket] = []
        for ws in watchers:
            try:
                await ws.send_json(payload)
            except WebSocketDisconnect:
                dead.append(ws)
            except RuntimeError:
                dead.append(ws)
            except Exception:  # pragma: no cover - network errors
                LOGGER.debug("Failed to broadcast to watcher.", exc_info=True)
                dead.append(ws)

        if dead:
            async with app.state.watchers_lock:
                for ws in dead:
                    app.state.watchers.discard(ws)

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.cleanup_task = asyncio.create_task(cleanup_loop())

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if app.state.cleanup_task:
            app.state.cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await app.state.cleanup_task

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok", "tasks": len(app.state.tasks)})

    @app.get("/tasks")
    async def list_tasks() -> Dict[str, Dict[str, Any]]:
        return {"tasks": await get_snapshot()}

    @app.post("/progress")
    async def progress(event: ProgressEvent, request: Request) -> Dict[str, Any]:
        if event.meta and "_token" in event.meta:
            meta_token = event.meta.pop("_token")
        else:
            meta_token = None
        query_token, header_token = extract_request_tokens(request)
        require_token(meta_token, query_token, header_token)
        now = time.time()
        event_dict = event.dict(exclude_none=True)
        event_dict.setdefault("timestamp", now)
        event_dict.setdefault("status", "update")

        terminal_record: tuple[Dict[str, Any], str, float] | None = None
        async with app.state.state_lock:
            task = app.state.tasks.get(
                event.task_id,
                {
                    "task_id": event.task_id,
                    "created_at": now,
                    "n": 0,
                    "total": None,
                    "status": "start",
                },
            )
            prev_status = task.get("status")
            # n_start anchors ETA on work-done-since-this-run-began rather
            # than n / elapsed_since_created_at. Without it, a resumed
            # download arriving with n=100MB and ~0s elapsed would report
            # an enormous rate and a near-zero ETA. Set once on the first
            # event we ever see for this task; subsequent events leave it.
            if "n_start" not in task:
                task["n_start"] = event.n if event.n is not None else (task.get("n") or 0)

            if event.desc is not None:
                task["desc"] = event.desc
            if event.total is not None:
                task["total"] = event.total
            if event.n is not None:
                task["n"] = event.n
            if event.unit is not None:
                task["unit"] = event.unit
            if event.meta is not None:
                task["meta"] = event.meta

            task["status"] = event_dict["status"]
            task["updated_at"] = now
            task["timestamp"] = event_dict["timestamp"]

            new_status = task["status"]
            if (
                app.state.history is not None
                and new_status in TERMINAL_STATUSES
                and prev_status not in TERMINAL_STATUSES
            ):
                # Record once per transition into a terminal state. Snapshot
                # the task dict because the live one mutates after we release
                # state_lock.
                terminal_record = (dict(task), new_status, now)

            if task["status"] == "close":
                task.setdefault("done_at", now)

            task.pop("recovered", None)
            task.pop("recovered_at", None)

            app.state.tasks[event.task_id] = task

        if terminal_record is not None:
            await app.state.history.record(*terminal_record)

        snapshot = await get_snapshot()
        await persist_state(snapshot)
        await broadcast(snapshot)

        return {"ok": True}

    @app.websocket("/ws")
    async def watch(ws: WebSocket) -> None:
        tokens: tuple[str, ...] = app.state.settings.api_tokens
        if tokens:
            candidate = ws.query_params.get("token") or _extract_bearer(ws.headers.get("authorization"))
            if not candidate or candidate not in tokens:
                await ws.close(code=4401, reason="Unauthorized")
                return
        await ws.accept()
        async with app.state.watchers_lock:
            app.state.watchers.add(ws)
        try:
            # Send current snapshot immediately.
            snapshot = await get_snapshot()
            await ws.send_json({"tasks": snapshot})
            while True:
                try:
                    await ws.receive_text()
                except WebSocketDisconnect:
                    break
                except Exception:
                    LOGGER.debug("WebSocket consumer terminated.", exc_info=True)
                    break
        finally:
            async with app.state.watchers_lock:
                app.state.watchers.discard(ws)

    @app.delete("/tasks/{task_id}")
    async def delete_task(task_id: str, request: Request) -> Dict[str, Any]:
        query_token, header_token = extract_request_tokens(request)
        require_token(query_token, header_token)
        async with app.state.state_lock:
            removed = app.state.tasks.pop(task_id, None)
        if removed:
            snapshot = await get_snapshot()
            await persist_state(snapshot)
            await broadcast(snapshot)
        return {"removed": bool(removed)}

    @app.get("/history")
    async def list_history(
        request: Request,
        limit: int = 50,
        before: int | None = None,
        task_id: str | None = None,
    ) -> Dict[str, Any]:
        query_token, header_token = extract_request_tokens(request)
        require_token(query_token, header_token)
        if app.state.history is None:
            return {"runs": [], "enabled": False}
        runs = await app.state.history.query(limit=limit, before_id=before, task_id=task_id)
        return {"runs": runs, "enabled": True}

    @app.get("/history/stats")
    async def history_stats(request: Request) -> Dict[str, Any]:
        query_token, header_token = extract_request_tokens(request)
        require_token(query_token, header_token)
        if app.state.history is None:
            return {"enabled": False, "total": 0, "by_status": {}}
        stats = await app.state.history.stats()
        return {"enabled": True, **stats}

    @app.get("/history/{history_id}")
    async def get_history(history_id: int, request: Request) -> Dict[str, Any]:
        query_token, header_token = extract_request_tokens(request)
        require_token(query_token, header_token)
        if app.state.history is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="History disabled")
        row = await app.state.history.get(history_id)
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        return row

    @app.delete("/history/{history_id}")
    async def delete_history(history_id: int, request: Request) -> Dict[str, Any]:
        query_token, header_token = extract_request_tokens(request)
        require_token(query_token, header_token)
        if app.state.history is None:
            return {"removed": False}
        removed = await app.state.history.delete(history_id)
        return {"removed": removed}

    @app.delete("/tasks")
    async def bulk_delete_tasks(
        request: Request,
        status: str | None = None,
        older_than: float | None = None,
    ) -> Dict[str, Any]:
        query_token, header_token = extract_request_tokens(request)
        require_token(query_token, header_token)
        async with app.state.state_lock:
            removed_ids: list[str] = []
            now = time.time()
            cutoff = now - older_than if older_than else None
            for task_id, data in list(app.state.tasks.items()):
                if status and data.get("status") != status:
                    continue
                updated_at = data.get("updated_at", data.get("created_at", now))
                if cutoff is not None and updated_at > cutoff:
                    continue
                removed_ids.append(task_id)
                app.state.tasks.pop(task_id, None)
        if removed_ids:
            snapshot = await get_snapshot()
            await persist_state(snapshot)
            await broadcast(snapshot)
        return {"removed": removed_ids}

    return app


def run_server(settings: ServerSettings | None = None) -> None:
    """Run the Progressista server using uvicorn."""

    settings = settings or ServerSettings()
    config = uvicorn.Config(
        "progressista.server:create_app",
        host=settings.host,
        port=settings.port,
        factory=True,
        reload=False,
        workers=1,
        log_level="info",
    )
    server = uvicorn.Server(config)
    display_host = settings.host
    if display_host in ("0.0.0.0", "::"):
        display_host = "localhost"
    LOGGER.info(
        "Starting Progressista server on %s:%s — dashboard at http://%s:%s/static/index.html",
        settings.host,
        settings.port,
        display_host,
        settings.port,
    )
    server.run()


__all__ = ["create_app", "run_server", "ProgressEvent"]
