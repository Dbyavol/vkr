from __future__ import annotations

import gzip
import hashlib
import json
from typing import Any

from app.core.logging import elapsed_ms, get_logger, start_timer
from app.db.session import SessionLocal
from app.schemas.preprocessing import (
    DatasetPayload,
    DatasetProfileRequest,
    DatasetRow,
)
from app.services.file_service import StorageAdapter, get_file
from app.services.import_parser import parse_dataset_bytes
from app.services.preprocessing_engine import preprocess_rows
from app.services.profiling_engine import profile_dataset

artifact_logger = get_logger("app.dataset_artifact")
ARTIFACT_VERSION = "v1"


def _gzip_json(payload: Any) -> bytes:
    return gzip.compress(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _ungzip_json(body: bytes) -> Any:
    return json.loads(gzip.decompress(body).decode("utf-8"))


def _save_artifact(key: str, payload: Any) -> str:
    adapter = StorageAdapter()
    body = _gzip_json(payload)
    adapter.put(key, body, "application/gzip")
    artifact_logger.info("artifact_saved storage_key=%r size_bytes=%r", key, len(body))
    return key


def _load_artifact(key: str) -> Any | None:
    adapter = StorageAdapter()
    if not adapter.exists(key):
        return None
    return _ungzip_json(adapter.download(key))


def build_raw_preview_artifact_key(dataset_file_id: int) -> str:
    return f"comparison-artifacts/{ARTIFACT_VERSION}/datasets/{dataset_file_id}/raw-preview.json.gz"


def build_raw_detailed_profile_artifact_key(dataset_file_id: int) -> str:
    return f"comparison-artifacts/{ARTIFACT_VERSION}/datasets/{dataset_file_id}/raw-profile-detailed.json.gz"


def _field_configs_signature(fields: list[dict[str, Any]]) -> str:
    normalized = json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _profile_options_signature(
    *,
    detail_level: str,
    histogram_bins: int,
    histogram_bins_by_field: dict[str, int] | None,
) -> str:
    payload = {
        "detail_level": detail_level,
        "histogram_bins": histogram_bins,
        "histogram_bins_by_field": histogram_bins_by_field or {},
    }
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def build_preprocessed_artifact_key(dataset_file_id: int, fields_signature: str) -> str:
    return f"comparison-artifacts/{ARTIFACT_VERSION}/datasets/{dataset_file_id}/preprocessed/{fields_signature}/dataset.json.gz"


def build_preprocessed_profile_artifact_key(dataset_file_id: int, fields_signature: str, profile_signature: str) -> str:
    return (
        f"comparison-artifacts/{ARTIFACT_VERSION}/datasets/{dataset_file_id}/preprocessed/"
        f"{fields_signature}/profiles/{profile_signature}.json.gz"
    )


def _load_dataset_source(dataset_file_id: int, filename: str | None = None) -> tuple[str, bytes]:
    with SessionLocal() as db:
        stored_file = get_file(db, dataset_file_id)
        if stored_file is None:
            raise ValueError("Файл датасета не найден")
        resolved_filename = filename or stored_file.original_name or "dataset"
        adapter = StorageAdapter()
        body = adapter.download(stored_file.storage_key)
    return resolved_filename, body


def ensure_raw_preview_artifact(
    *,
    dataset_file_id: int,
    filename: str | None = None,
    source_body: bytes | None = None,
) -> dict[str, Any]:
    key = build_raw_preview_artifact_key(dataset_file_id)
    cached = _load_artifact(key)
    if cached is not None:
        artifact_logger.info("raw_preview_artifact_cache_hit dataset_file_id=%r storage_key=%r", dataset_file_id, key)
        return cached

    started_at = start_timer()
    resolved_filename = filename or "dataset"
    body = source_body
    if body is None:
        resolved_filename, body = _load_dataset_source(dataset_file_id, filename)

    preview = parse_dataset_bytes(resolved_filename, body).model_dump()
    _save_artifact(key, preview)
    artifact_logger.info(
        "raw_preview_artifact_built dataset_file_id=%r filename=%r rows_total=%r duration_ms=%.2f",
        dataset_file_id,
        resolved_filename,
        preview.get("rows_total"),
        elapsed_ms(started_at),
    )
    return preview


def load_raw_detailed_profile_artifact(dataset_file_id: int) -> dict[str, Any] | None:
    key = build_raw_detailed_profile_artifact_key(dataset_file_id)
    cached = _load_artifact(key)
    if cached is not None:
        artifact_logger.info("raw_detailed_profile_cache_hit dataset_file_id=%r storage_key=%r", dataset_file_id, key)
    return cached


def build_and_cache_raw_detailed_profile_artifact(
    *,
    dataset_file_id: int,
    filename: str | None = None,
    source_body: bytes | None = None,
) -> dict[str, Any]:
    started_at = start_timer()
    preview = ensure_raw_preview_artifact(
        dataset_file_id=dataset_file_id,
        filename=filename,
        source_body=source_body,
    )
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
    _save_artifact(build_raw_detailed_profile_artifact_key(dataset_file_id), profile)
    artifact_logger.info(
        "raw_detailed_profile_built dataset_file_id=%r rows_total=%r duration_ms=%.2f",
        dataset_file_id,
        preview.get("rows_total"),
        elapsed_ms(started_at),
    )
    return profile


def ensure_preprocessed_artifact(
    *,
    dataset_file_id: int,
    fields: list[dict[str, Any]],
    raw_preview: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    fields_signature = _field_configs_signature(fields)
    key = build_preprocessed_artifact_key(dataset_file_id, fields_signature)
    cached = _load_artifact(key)
    if cached is not None:
        artifact_logger.info(
            "preprocessed_artifact_cache_hit dataset_file_id=%r fields_signature=%r storage_key=%r",
            dataset_file_id,
            fields_signature,
            key,
        )
        return cached, fields_signature

    started_at = start_timer()
    preview = raw_preview or ensure_raw_preview_artifact(dataset_file_id=dataset_file_id)
    preprocessing = preprocess_rows(
        rows_input=preview["normalized_dataset"]["rows"],
        fields_input=fields,
        options_input={
            "drop_duplicate_rows": True,
            "duplicate_keys": [],
            "keep_columns_not_in_config": False,
            "preserve_original_values": False,
        },
    )
    _save_artifact(key, preprocessing)
    artifact_logger.info(
        "preprocessed_artifact_built dataset_file_id=%r fields_signature=%r rows_output=%r duration_ms=%.2f",
        dataset_file_id,
        fields_signature,
        len(preprocessing.get("dataset") or []),
        elapsed_ms(started_at),
    )
    return preprocessing, fields_signature


def ensure_preprocessed_profile_artifact(
    *,
    dataset_file_id: int,
    fields_signature: str,
    rows: list[dict[str, Any]],
    detail_level: str,
    histogram_bins: int,
    histogram_bins_by_field: dict[str, int] | None = None,
) -> dict[str, Any]:
    profile_signature = _profile_options_signature(
        detail_level=detail_level,
        histogram_bins=histogram_bins,
        histogram_bins_by_field=histogram_bins_by_field,
    )
    key = build_preprocessed_profile_artifact_key(dataset_file_id, fields_signature, profile_signature)
    cached = _load_artifact(key)
    if cached is not None:
        artifact_logger.info(
            "preprocessed_profile_cache_hit dataset_file_id=%r fields_signature=%r profile_signature=%r storage_key=%r",
            dataset_file_id,
            fields_signature,
            profile_signature,
            key,
        )
        return cached

    started_at = start_timer()
    profile = profile_dataset(
        DatasetProfileRequest(
            dataset=DatasetPayload(rows=[DatasetRow.model_validate(row) for row in rows]),
            max_unique_values=30,
            histogram_bins=histogram_bins,
            histogram_bins_by_field=histogram_bins_by_field or {},
            detail_level=detail_level,  # type: ignore[arg-type]
        )
    ).model_dump()
    _save_artifact(key, profile)
    artifact_logger.info(
        "preprocessed_profile_built dataset_file_id=%r fields_signature=%r detail_level=%r rows_total=%r duration_ms=%.2f",
        dataset_file_id,
        fields_signature,
        detail_level,
        len(rows),
        elapsed_ms(started_at),
    )
    return profile
