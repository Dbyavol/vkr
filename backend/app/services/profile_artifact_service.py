from __future__ import annotations

import json
from typing import Any

from app.core.logging import elapsed_ms, get_logger, start_timer
from app.db.session import SessionLocal
from app.services.file_service import StorageAdapter, get_file
from app.services.import_parser import parse_dataset_bytes
from app.services.profiling_engine import profile_dataset
from app.schemas.preprocessing import DatasetPayload, DatasetProfileRequest, DatasetRow

artifact_logger = get_logger("app.profile_artifact")
ARTIFACT_VERSION = "v1"


def build_detailed_profile_artifact_key(dataset_file_id: int) -> str:
    return f"comparison-profile-artifacts/{ARTIFACT_VERSION}/dataset-{dataset_file_id}-detailed.json"


def load_cached_detailed_profile(dataset_file_id: int) -> dict[str, Any] | None:
    adapter = StorageAdapter()
    key = build_detailed_profile_artifact_key(dataset_file_id)
    if not adapter.exists(key):
        return None
    body = adapter.download(key)
    artifact_logger.info("profile_artifact_cache_hit dataset_file_id=%r storage_key=%r", dataset_file_id, key)
    return json.loads(body.decode("utf-8"))


def save_cached_detailed_profile(dataset_file_id: int, payload: dict[str, Any]) -> str:
    adapter = StorageAdapter()
    key = build_detailed_profile_artifact_key(dataset_file_id)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    adapter.put(key, body, "application/json")
    artifact_logger.info(
        "profile_artifact_saved dataset_file_id=%r storage_key=%r size_bytes=%r",
        dataset_file_id,
        key,
        len(body),
    )
    return key


def build_and_cache_detailed_profile_artifact(
    *,
    dataset_file_id: int,
    filename: str | None = None,
) -> dict[str, Any]:
    started_at = start_timer()
    with SessionLocal() as db:
        stored_file = get_file(db, dataset_file_id)
        if stored_file is None:
            raise ValueError("Файл датасета не найден")
        resolved_filename = filename or stored_file.original_name or "dataset"
        adapter = StorageAdapter()
        body = adapter.download(stored_file.storage_key)

    preview = parse_dataset_bytes(resolved_filename, body).model_dump()
    rows = preview["normalized_dataset"]["rows"]
    profile = profile_dataset(
        DatasetProfileRequest(
            dataset=DatasetPayload(rows=[DatasetRow.model_validate(row) for row in rows]),
            max_unique_values=30,
            histogram_bins=8,
            histogram_bins_by_field={},
            detail_level="detailed",
        )
    ).model_dump()
    save_cached_detailed_profile(dataset_file_id, profile)
    artifact_logger.info(
        "profile_artifact_build_completed dataset_file_id=%r filename=%r rows_total=%r duration_ms=%.2f",
        dataset_file_id,
        resolved_filename,
        preview.get("rows_total"),
        elapsed_ms(started_at),
    )
    return profile
