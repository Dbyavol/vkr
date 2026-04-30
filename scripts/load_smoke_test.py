from __future__ import annotations

import argparse
import json
import math
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def request_once(url: str, timeout: float) -> tuple[bool, float, int]:
    started = time.perf_counter()
    status_code = 0
    ok = False
    try:
        request = urllib.request.Request(url=url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status_code = int(response.getcode() or 0)
            ok = 200 <= status_code < 400
    except urllib.error.HTTPError as exc:
        status_code = int(exc.code)
    except Exception:
        status_code = 0
    duration_ms = (time.perf_counter() - started) * 1000.0
    return ok, duration_ms, status_code


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int(math.floor(fraction * (len(ordered) - 1)))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser(description="Lightweight HTTP load smoke test")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Base URL for API")
    parser.add_argument("--path", action="append", default=["/health", "/api/v1/system/dashboard"], help="Path to test (can be repeated)")
    parser.add_argument("--requests", type=int, default=120, help="Total requests per path")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent workers")
    parser.add_argument("--timeout", type=float, default=8.0, help="Request timeout, seconds")
    args = parser.parse_args()

    report: dict[str, dict[str, float | int]] = {}

    for path in args.path:
        url = f"{args.base_url.rstrip('/')}/{path.lstrip('/')}"
        durations: list[float] = []
        errors = 0
        statuses: dict[int, int] = {}

        with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
            futures = [pool.submit(request_once, url, args.timeout) for _ in range(max(1, args.requests))]
            for future in as_completed(futures):
                ok, duration_ms, status_code = future.result()
                durations.append(duration_ms)
                statuses[status_code] = statuses.get(status_code, 0) + 1
                if not ok:
                    errors += 1

        total = len(durations)
        avg_ms = sum(durations) / total if total else 0.0
        report[path] = {
            "requests": total,
            "errors": errors,
            "error_rate_pct": round(errors / total * 100.0 if total else 0.0, 2),
            "avg_ms": round(avg_ms, 2),
            "p95_ms": round(percentile(durations, 0.95), 2),
            "p99_ms": round(percentile(durations, 0.99), 2),
            "min_ms": round(min(durations) if durations else 0.0, 2),
            "max_ms": round(max(durations) if durations else 0.0, 2),
            "statuses": statuses,
        }

    print(json.dumps({"base_url": args.base_url, "concurrency": args.concurrency, "report": report}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
