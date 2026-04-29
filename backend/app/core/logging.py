from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import perf_counter
from typing import Any

from fastapi import Request

from app.core.config import get_settings
from app.services.security import decode_access_token

settings = get_settings()

_configured = False


def configure_logging() -> None:
    global _configured
    if _configured:
        return

    log_path = Path(settings.log_file)
    if not log_path.is_absolute():
        log_path = Path.cwd() / log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


def get_request_identity(request: Request) -> dict[str, Any]:
    authorization = request.headers.get("authorization")
    if not authorization or not authorization.lower().startswith("bearer "):
        return {"user_id": None, "user_email": None, "user_role": None}

    token = authorization.split(" ", 1)[1]
    try:
        payload = decode_access_token(token)
    except ValueError:
        return {"user_id": None, "user_email": None, "user_role": None}

    return {
        "user_id": payload.get("sub"),
        "user_email": payload.get("email"),
        "user_role": payload.get("role"),
    }


def format_log_context(**context: Any) -> str:
    parts: list[str] = []
    for key, value in context.items():
        if value is None:
            continue
        if isinstance(value, float):
            value = f"{value:.2f}"
        parts.append(f"{key}={value!r}")
    return " ".join(parts)


def log_audit_event(event: str, **context: Any) -> None:
    logger = get_logger("app.audit")
    logger.info("%s %s", event, format_log_context(**context))


def start_timer() -> float:
    return perf_counter()


def elapsed_ms(started_at: float) -> float:
    return (perf_counter() - started_at) * 1000
