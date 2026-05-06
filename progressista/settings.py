"""Runtime settings for Progressista server and client."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(slots=True)
class ServerSettings:
    """Settings that control the FastAPI server runtime."""

    host: str = os.getenv("PROGRESSISTA_HOST", "0.0.0.0")
    port: int = _int_env("PROGRESSISTA_PORT", 8000)
    storage_path: str | None = os.getenv("PROGRESSISTA_STORAGE_PATH")
    history_db_path: str | None = os.getenv("PROGRESSISTA_HISTORY_DB_PATH")
    cleanup_interval: float = _float_env("PROGRESSISTA_CLEANUP_INTERVAL", 5.0)
    retention_seconds: float = _float_env("PROGRESSISTA_RETENTION_SECONDS", 86400.0)
    stale_seconds: float = _float_env("PROGRESSISTA_STALE_SECONDS", 0.0)
    max_task_age: float = _float_env("PROGRESSISTA_MAX_TASK_AGE", 0.0)
    allow_origins: tuple[str, ...] = ()
    api_tokens: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        origins = os.getenv("PROGRESSISTA_ALLOW_ORIGINS")
        if origins:
            self.allow_origins = tuple(o.strip() for o in origins.split(",") if o.strip())
        else:
            self.allow_origins = ()
        tokens = os.getenv("PROGRESSISTA_API_TOKENS")
        if tokens:
            self.api_tokens = tuple(t.strip() for t in tokens.split(",") if t.strip())
        else:
            token = os.getenv("PROGRESSISTA_API_TOKEN")
            self.api_tokens = (token,) if token else ()


@dataclass(slots=True)
class ClientSettings:
    """Settings for the RemoteTqdm client."""

    server_url: str = os.getenv("PROGRESSISTA_SERVER_URL", "http://localhost:8000/progress")
    push_interval: float = _float_env("PROGRESSISTA_PUSH_INTERVAL", 0.25)
    request_timeout: float = _float_env("PROGRESSISTA_REQUEST_TIMEOUT", 2.0)
    api_token: str | None = os.getenv("PROGRESSISTA_API_TOKEN")
