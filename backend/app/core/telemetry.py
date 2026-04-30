from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


WINDOW_SIZE = 500


@dataclass
class ModuleWindowStats:
    durations: deque[float] = field(default_factory=lambda: deque(maxlen=WINDOW_SIZE))
    requests: int = 0
    errors: int = 0
    total_duration_ms: float = 0.0


_lock = Lock()
_module_stats: dict[str, ModuleWindowStats] = {}


def _module_from_path(path: str) -> str:
    if path in {"", "/", "/health"}:
        return "system"
    if path.startswith("/api/v1/auth") or path.startswith("/api/v1/users") or path.startswith("/api/v1/admin"):
        return "auth"
    if path.startswith("/api/v1/pipeline"):
        return "pipeline"
    if path.startswith("/api/v1/reports"):
        return "reports"
    if path.startswith("/api/v1/files") or path.startswith("/api/v1/projects") or path.startswith("/api/v1/comparison-history"):
        return "storage"
    if path.startswith("/api/v1/object-types") or path.startswith("/api/v1/objects"):
        return "objects"
    if path.startswith("/api/v1/system"):
        return "system"
    return "other"


def record_request(path: str, duration_ms: float, status_code: int) -> None:
    module = _module_from_path(path)
    with _lock:
        stats = _module_stats.setdefault(module, ModuleWindowStats())
        stats.requests += 1
        stats.total_duration_ms += duration_ms
        stats.durations.append(float(duration_ms))
        if status_code >= 400:
            stats.errors += 1


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(0.95 * (len(ordered) - 1))
    return float(ordered[index])


def get_telemetry_snapshot() -> dict[str, Any]:
    with _lock:
        modules_payload: list[dict[str, Any]] = []
        total_requests = 0
        total_errors = 0
        total_duration_ms = 0.0

        for module, stats in _module_stats.items():
            avg_ms = stats.total_duration_ms / stats.requests if stats.requests else 0.0
            error_rate_pct = (stats.errors / stats.requests * 100.0) if stats.requests else 0.0
            durations = list(stats.durations)
            modules_payload.append(
                {
                    "module": module,
                    "requests": stats.requests,
                    "errors": stats.errors,
                    "error_rate_pct": round(error_rate_pct, 2),
                    "avg_ms": round(avg_ms, 2),
                    "p95_ms": round(_p95(durations), 2),
                }
            )
            total_requests += stats.requests
            total_errors += stats.errors
            total_duration_ms += stats.total_duration_ms

    modules_payload.sort(key=lambda item: item["requests"], reverse=True)
    overall_avg_ms = total_duration_ms / total_requests if total_requests else 0.0
    overall_error_rate_pct = (total_errors / total_requests * 100.0) if total_requests else 0.0
    return {
        "window_size": WINDOW_SIZE,
        "overall": {
            "requests": total_requests,
            "errors": total_errors,
            "error_rate_pct": round(overall_error_rate_pct, 2),
            "avg_ms": round(overall_avg_ms, 2),
        },
        "modules": modules_payload,
    }


def reset_telemetry() -> None:
    with _lock:
        _module_stats.clear()
