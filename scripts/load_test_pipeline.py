from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import httpx


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("BACKEND_DATABASE_URL", f"sqlite:///{(ROOT_DIR / 'backend' / 'load_test.db').as_posix()}")
os.environ.setdefault("BACKEND_S3_ENDPOINT_URL", "")
os.environ.setdefault("BACKEND_LOCAL_STORAGE_DIR", str(ROOT_DIR / "backend" / "load-test-storage"))
os.environ.setdefault("BACKEND_BOOTSTRAP_ADMIN_EMAIL", "admin@example.com")
os.environ.setdefault("BACKEND_BOOTSTRAP_ADMIN_PASSWORD", "admin12345")
os.environ.setdefault("BACKEND_JWT_SECRET", "load-test-secret")

from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.schemas.pipeline import CriterionConfig, FieldConfig, PipelineConfig, PipelineRequest
from app.services.pipeline_engine import run_pipeline_via_services, upload_and_profile_dataset
from app.services.user_service import bootstrap_admin


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(math.ceil(fraction * len(ordered)) - 1)))
    return ordered[index]


def summarize_metric_runs(runs: list[dict[str, Any]], metric_key: str) -> dict[str, float]:
    values = [float(item[metric_key]) for item in runs if metric_key in item]
    errors = sum(1 for item in runs if not item.get("ok", True))
    total = len(runs)
    return {
        "avg_ms": round(statistics.mean(values), 2) if values else 0.0,
        "min_ms": round(min(values), 2) if values else 0.0,
        "max_ms": round(max(values), 2) if values else 0.0,
        "p95_ms": round(percentile(values, 0.95), 2) if values else 0.0,
        "p99_ms": round(percentile(values, 0.99), 2) if values else 0.0,
        "errors": errors,
        "error_rate_pct": round((errors / total) * 100, 2) if total else 0.0,
    }


def generate_dataset(rows: int) -> bytes:
    header = "name,price,area,rooms,quality,district\n"
    lines = [header]
    districts = ["center", "north", "south", "west", "east"]
    qualities = ["A", "B", "C", "D"]
    for index in range(rows):
        price = 80_000 + (index % 400) * 250
        area = 25 + (index % 120) * 0.75
        rooms = 1 + (index % 5)
        quality = qualities[index % len(qualities)]
        district = districts[index % len(districts)]
        lines.append(f"Object {index + 1},{price},{area:.2f},{rooms},{quality},{district}\n")
    return "".join(lines).encode("utf-8")


def build_request(filename: str) -> PipelineRequest:
    return PipelineRequest(
        filename=filename,
        config=PipelineConfig(
            fields=[
                FieldConfig(key="name", field_type="text"),
                FieldConfig(key="price", field_type="float", normalization="minmax"),
                FieldConfig(key="area", field_type="float", normalization="minmax"),
                FieldConfig(key="rooms", field_type="integer", normalization="minmax"),
                FieldConfig(
                    key="quality",
                    field_type="categorical",
                    encoding="ordinal",
                    ordinal_map={"A": 1.0, "B": 0.75, "C": 0.5, "D": 0.25},
                ),
                FieldConfig(
                    key="district",
                    field_type="categorical",
                    encoding="ordinal",
                    ordinal_map={"center": 1.0, "north": 0.8, "west": 0.6, "east": 0.4, "south": 0.2},
                ),
            ],
            criteria=[
                CriterionConfig(key="price", name="Цена", weight=0.35, type="numeric", direction="minimize"),
                CriterionConfig(key="area", name="Площадь", weight=0.25, type="numeric", direction="maximize"),
                CriterionConfig(key="rooms", name="Комнаты", weight=0.15, type="numeric", direction="maximize"),
                CriterionConfig(key="quality", name="Качество", weight=0.15, type="categorical", direction="maximize"),
                CriterionConfig(key="district", name="Район", weight=0.10, type="categorical", direction="maximize"),
            ],
            analysis_mode="rating",
            top_n=10,
        ),
    )


async def run_single_internal_case(filename: str, body: bytes) -> dict[str, Any]:
    started_at = time.perf_counter()
    profile_started_at = time.perf_counter()
    await upload_and_profile_dataset(filename=filename, body=body, detail_level="summary")
    profile_ms = (time.perf_counter() - profile_started_at) * 1000

    pipeline_started_at = time.perf_counter()
    result = await run_pipeline_via_services(
        filename=filename,
        body=body,
        payload=build_request(filename),
    )
    pipeline_ms = (time.perf_counter() - pipeline_started_at) * 1000
    total_ms = (time.perf_counter() - started_at) * 1000

    return {
        "ok": True,
        "profile_ms": round(profile_ms, 2),
        "pipeline_ms": round(pipeline_ms, 2),
        "total_ms": round(total_ms, 2),
        "ranking_count": len(result.ranking),
    }


async def run_internal_benchmark(rows: int, iterations: int, concurrency: int) -> dict[str, Any]:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        bootstrap_admin(db)

    body = generate_dataset(rows)
    filename = f"load-{rows}.csv"
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded_run() -> dict[str, Any]:
        async with semaphore:
            return await run_single_internal_case(filename, body)

    runs = await asyncio.gather(*(bounded_run() for _ in range(iterations)))
    return {
        "mode": "internal",
        "rows": rows,
        "iterations": iterations,
        "concurrency": concurrency,
        "summary": {
            "profile": summarize_metric_runs(runs, "profile_ms"),
            "pipeline": summarize_metric_runs(runs, "pipeline_ms"),
            "total": summarize_metric_runs(runs, "total_ms"),
        },
        "runs": runs,
    }


async def run_http_pipeline_case(
    *,
    client: httpx.AsyncClient,
    filename: str,
    body: bytes,
    timeout_s: float,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        upload_started_at = time.perf_counter()
        upload_response = await client.post(
            "/api/v1/pipeline/upload-profile",
            files={"file": (filename, body, "text/csv")},
            data={"detail_level": "summary"},
            timeout=timeout_s,
        )
        upload_ms = (time.perf_counter() - upload_started_at) * 1000
        upload_response.raise_for_status()
        dataset_file_id = upload_response.json()["dataset_file_id"]

        run_started_at = time.perf_counter()
        run_response = await client.post(
            "/api/v1/pipeline/run-stored",
            json={
                "filename": filename,
                "dataset_file_id": dataset_file_id,
                "config": build_request(filename).config.model_dump(),
            },
            timeout=timeout_s,
        )
        run_ms = (time.perf_counter() - run_started_at) * 1000
        run_response.raise_for_status()
        ranking = run_response.json().get("ranking") or []
        total_ms = (time.perf_counter() - started_at) * 1000
        return {
            "ok": True,
            "upload_ms": round(upload_ms, 2),
            "run_ms": round(run_ms, 2),
            "total_ms": round(total_ms, 2),
            "ranking_count": len(ranking),
        }
    except Exception as exc:
        total_ms = (time.perf_counter() - started_at) * 1000
        return {
            "ok": False,
            "upload_ms": 0.0,
            "run_ms": 0.0,
            "total_ms": round(total_ms, 2),
            "error": str(exc),
        }


async def run_http_pipeline_benchmark(
    *,
    base_url: str,
    rows: int,
    iterations: int,
    concurrency: int,
    timeout_s: float,
) -> dict[str, Any]:
    body = generate_dataset(rows)
    filename = f"http-load-{rows}.csv"
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), follow_redirects=True) as client:
        async def bounded_run() -> dict[str, Any]:
            async with semaphore:
                return await run_http_pipeline_case(client=client, filename=filename, body=body, timeout_s=timeout_s)

        runs = await asyncio.gather(*(bounded_run() for _ in range(iterations)))

    return {
        "mode": "http_pipeline",
        "base_url": base_url,
        "rows": rows,
        "iterations": iterations,
        "concurrency": concurrency,
        "summary": {
            "upload": summarize_metric_runs(runs, "upload_ms"),
            "run": summarize_metric_runs(runs, "run_ms"),
            "total": summarize_metric_runs(runs, "total_ms"),
        },
        "runs": runs,
    }


async def run_matrix(
    *,
    mode: str,
    rows_values: list[int],
    concurrency_values: list[int],
    iterations: int,
    base_url: str,
    timeout_s: float,
) -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    for rows in rows_values:
        for concurrency in concurrency_values:
            if mode == "internal":
                result = await run_internal_benchmark(rows=rows, iterations=iterations, concurrency=concurrency)
            else:
                result = await run_http_pipeline_benchmark(
                    base_url=base_url,
                    rows=rows,
                    iterations=iterations,
                    concurrency=concurrency,
                    timeout_s=timeout_s,
                )
            cases.append(result)
    return {
        "mode": mode,
        "rows_values": rows_values,
        "concurrency_values": concurrency_values,
        "iterations": iterations,
        "cases": cases,
    }


def parse_int_list(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Load testing suite for the comparative analysis system.")
    parser.add_argument("--mode", choices=["internal", "http_pipeline"], default="internal")
    parser.add_argument("--rows", type=int, default=5000, help="Single dataset size for one benchmark.")
    parser.add_argument("--rows-list", type=str, default="", help="Comma-separated dataset sizes for matrix benchmark.")
    parser.add_argument("--iterations", type=int, default=5, help="Number of benchmark iterations per case.")
    parser.add_argument("--concurrency", type=int, default=1, help="Single concurrency level for one benchmark.")
    parser.add_argument("--concurrency-list", type=str, default="", help="Comma-separated concurrency values for matrix benchmark.")
    parser.add_argument("--base-url", type=str, default="http://localhost:8050", help="Backend base URL for HTTP mode.")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds for HTTP mode.")
    parser.add_argument("--output", type=str, default="", help="Optional output JSON file.")
    args = parser.parse_args()

    rows_values = parse_int_list(args.rows_list) if args.rows_list else [args.rows]
    concurrency_values = parse_int_list(args.concurrency_list) if args.concurrency_list else [args.concurrency]

    payload = asyncio.run(
        run_matrix(
            mode=args.mode,
            rows_values=rows_values,
            concurrency_values=concurrency_values,
            iterations=args.iterations,
            base_url=args.base_url,
            timeout_s=args.timeout,
        )
    )
    result_json = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        Path(args.output).write_text(result_json, encoding="utf-8")
    else:
        print(result_json)


if __name__ == "__main__":
    main()
