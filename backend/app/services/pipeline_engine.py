from __future__ import annotations

import json
from typing import Any

from app.core.logging import elapsed_ms, get_logger, log_audit_event, start_timer
from app.core.telemetry import get_telemetry_snapshot
from app.db.session import SessionLocal
from app.services.dataset_artifact_service import (
    build_and_cache_raw_detailed_profile_artifact,
    ensure_preprocessed_artifact,
    ensure_preprocessed_profile_artifact,
    ensure_raw_preview_artifact,
    load_raw_detailed_profile_artifact,
)
from app.schemas.analysis import AnalysisDataset, AnalysisFilters, AnalysisRequest, CriterionConfig as AnalysisCriterionConfig, DatasetObject
from app.schemas.files import ComparisonHistoryCreate
from app.schemas.pipeline import PipelineRequest, PipelineRunResponse
from app.schemas.preprocessing import (
    DatasetPayload,
    DatasetProfileRequest,
    DatasetRow,
    ProfileDetailLevel,
)
from app.services.analysis_engine import run_comparative_analysis
from app.services.file_service import StorageAdapter, create_comparison_history, create_file_record, get_file, storage_stats
from app.services.import_parser import parse_dataset_bytes
from app.services.profiling_engine import profile_dataset
from app.services.preprocessing_engine import preprocess_rows
from app.services.security import decode_access_token
from app.services.user_service import admin_stats, get_user


PREVIEW_NORMALIZED_ROW_LIMIT = 200
pipeline_logger = get_logger("app.pipeline")


def _object_title(row: dict[str, Any]) -> str:
    if row.get("title") not in (None, ""):
        return str(row["title"])
    values = row.get("values", {})
    for key in ("name", "title", "object_name", "label", "название", "наименование"):
        value = values.get(key)
        if value not in (None, ""):
            return str(value)
    return f"Объект {row['id']}"


def _analysis_dataset(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "objects": [
            {
                "id": str(row["id"]),
                "title": _object_title(row),
                "attributes": row["values"],
            }
            for row in rows
        ]
    }


async def fetch_preview(*, filename: str, body: bytes) -> dict[str, Any]:
    started_at = start_timer()
    preview = parse_dataset_bytes(filename, body).model_dump()
    pipeline_logger.info(
        "fetch_preview_completed filename=%r size_bytes=%r rows_total=%r duration_ms=%.2f",
        filename,
        len(body),
        preview.get("rows_total"),
        elapsed_ms(started_at),
    )
    return preview


def lightweight_preview(preview: dict[str, Any], limit: int = PREVIEW_NORMALIZED_ROW_LIMIT) -> dict[str, Any]:
    trimmed = dict(preview)
    normalized_dataset = preview.get("normalized_dataset") or {}
    rows = list(normalized_dataset.get("rows") or [])
    trimmed["normalized_dataset"] = {"rows": rows[:limit]}
    trimmed["preview_rows"] = list(preview.get("preview_rows") or [])[: min(limit, 50)]
    if len(rows) > limit:
        warnings = list(trimmed.get("warnings") or [])
        warnings.append(
            f"Для ускорения интерфейса показаны первые {limit} строк. Полный датасет сохранен в хранилище и используется при расчете."
        )
        trimmed["warnings"] = warnings
    return trimmed


async def upload_dataset_file(*, filename: str, body: bytes) -> dict[str, Any]:
    started_at = start_timer()
    adapter = StorageAdapter()
    key, checksum = adapter.upload(filename, "application/octet-stream", body, prefix="comparison-dataset-source")
    with SessionLocal() as db:
        item = create_file_record(
            db,
            original_name=filename,
            content_type="application/octet-stream",
            purpose="comparison-dataset-source",
            storage_key=key,
            size_bytes=len(body),
            checksum=checksum,
        )
        pipeline_logger.info(
            "upload_dataset_file_completed filename=%r file_id=%r size_bytes=%r duration_ms=%.2f",
            filename,
            item.id,
            len(body),
            elapsed_ms(started_at),
        )
        return {"id": item.id, "original_name": item.original_name, "storage_key": item.storage_key}


async def fetch_stored_file_metadata(*, file_id: int) -> dict[str, Any]:
    with SessionLocal() as db:
        item = get_file(db, file_id)
        if item is None:
            raise ValueError("Файл не найден")
        return {
            "id": item.id,
            "original_name": item.original_name,
            "content_type": item.content_type,
            "storage_key": item.storage_key,
        }


async def fetch_stored_file_body(*, file_id: int) -> bytes:
    with SessionLocal() as db:
        item = get_file(db, file_id)
        if item is None:
            raise ValueError("Файл не найден")
        adapter = StorageAdapter()
        return adapter.download(item.storage_key)


async def run_preprocessing(*, rows: list[dict[str, Any]], fields: list[dict[str, Any]]) -> dict[str, Any]:
    started_at = start_timer()
    result = preprocess_rows(
        rows_input=rows,
        fields_input=fields,
        options_input={
            "drop_duplicate_rows": True,
            "duplicate_keys": [],
            "keep_columns_not_in_config": False,
            "preserve_original_values": False,
        },
    )
    pipeline_logger.info(
        "run_preprocessing_completed input_rows=%r fields_count=%r output_rows=%r duration_ms=%.2f",
        len(rows),
        len(fields),
        len(result.get("dataset") or []),
        elapsed_ms(started_at),
    )
    return result


async def profile_imported_dataset(
    *,
    rows: list[dict[str, Any]],
    histogram_bins: int = 8,
    histogram_bins_by_field: dict[str, int] | None = None,
    detail_level: ProfileDetailLevel = "detailed",
) -> dict[str, Any]:
    started_at = start_timer()
    payload = DatasetProfileRequest(
        dataset=DatasetPayload(rows=[DatasetRow.model_validate(row) for row in rows]),
        max_unique_values=30,
        histogram_bins=histogram_bins,
        histogram_bins_by_field=histogram_bins_by_field or {},
        detail_level=detail_level,
    )
    result = profile_dataset(payload).model_dump()
    pipeline_logger.info(
        "profile_imported_dataset_completed rows=%r fields_count=%r histogram_bins=%r detail_level=%r duration_ms=%.2f",
        len(rows),
        len(result.get("fields") or []),
        histogram_bins,
        detail_level,
        elapsed_ms(started_at),
    )
    return result


async def fetch_dataset_profile(*, filename: str, body: bytes, detail_level: ProfileDetailLevel = "detailed") -> dict[str, Any]:
    preview = await fetch_preview(filename=filename, body=body)
    profile = await profile_imported_dataset(rows=preview["normalized_dataset"]["rows"], detail_level=detail_level)
    return {"preview": lightweight_preview(preview), "profile": profile}


def build_preview_only_profile(preview: dict[str, Any]) -> dict[str, Any]:
    columns = list(preview.get("columns") or [])
    numeric_like_types = {"numeric", "integer", "float"}
    rows_total = int(preview.get("rows_total") or 0)
    return {
        "rows_total": rows_total,
        "detail_level": "summary",
        "fields": [],
        "quality": {
            "score": 0.0,
            "level": "info",
            "readiness_label": "Расширенная аналитика будет рассчитана на этапе подготовки",
            "analytic_fields_count": sum(1 for column in columns if column.get("inferred_type") in numeric_like_types | {"categorical", "binary", "datetime"}),
            "numeric_fields_count": sum(1 for column in columns if column.get("inferred_type") in numeric_like_types),
            "categorical_fields_count": sum(1 for column in columns if column.get("inferred_type") == "categorical"),
            "text_fields_count": sum(1 for column in columns if column.get("inferred_type") == "text"),
            "total_missing_values": sum(int(column.get("missing_count") or 0) for column in columns),
            "total_outliers_iqr": 0,
            "issues": [],
        },
        "recommended_weights": {},
        "weight_notes": ["Подробный профиль и графики считаются отдельно и не блокируют первый экран."],
        "missing_matrix_preview": [],
        "correlation_matrix": [],
    }


async def upload_and_profile_dataset(
    *,
    filename: str,
    body: bytes,
    detail_level: ProfileDetailLevel = "detailed",
) -> dict[str, Any]:
    uploaded = await upload_dataset_file(filename=filename, body=body)
    preview = ensure_raw_preview_artifact(
        dataset_file_id=int(uploaded["id"]),
        filename=filename,
        source_body=body,
    )
    if detail_level == "detailed":
        profile = await profile_imported_dataset(
            rows=preview["normalized_dataset"]["rows"],
            detail_level=detail_level,
        )
    else:
        profile = build_preview_only_profile(preview)
    profiled = {"preview": lightweight_preview(preview), "profile": profile}
    return {"dataset_file_id": uploaded["id"], **profiled}


async def fetch_stored_dataset_profile(
    *,
    dataset_file_id: int,
    filename: str | None,
    histogram_bins: int = 8,
    histogram_bins_by_field: dict[str, int] | None = None,
    detail_level: ProfileDetailLevel = "detailed",
) -> dict[str, Any]:
    raw_preview = ensure_raw_preview_artifact(dataset_file_id=dataset_file_id, filename=filename)
    resolved_filename = filename or raw_preview.get("filename") or "dataset"
    if detail_level == "detailed" and histogram_bins == 8 and not histogram_bins_by_field:
        cached = load_raw_detailed_profile_artifact(dataset_file_id)
        if cached is not None:
            pipeline_logger.info(
                "fetch_stored_dataset_profile_cache_hit dataset_file_id=%r filename=%r",
                dataset_file_id,
                resolved_filename,
            )
            return {"dataset_file_id": dataset_file_id, "preview": lightweight_preview(raw_preview), "profile": cached}
    profile = await profile_imported_dataset(
        rows=raw_preview["normalized_dataset"]["rows"],
        histogram_bins=histogram_bins,
        histogram_bins_by_field=histogram_bins_by_field,
        detail_level=detail_level,
    )
    return {"dataset_file_id": dataset_file_id, "preview": lightweight_preview(raw_preview), "profile": profile}


async def analyze_dataset(
    *,
    rows: list[dict[str, Any]],
    criteria: list[dict[str, Any]],
    target_row_id: str | None,
    mode: str,
    top_n: int,
    filter_criteria: dict[str, Any] | None,
    include_stability_scenarios: bool,
    stability_variation_pct: float,
) -> dict[str, Any]:
    started_at = start_timer()
    dataset = _analysis_dataset(rows)
    payload = AnalysisRequest(
        dataset=AnalysisDataset(
            objects=[DatasetObject(id=obj["id"], title=obj["title"], attributes=obj["attributes"]) for obj in dataset["objects"]]
        ),
        criteria=[AnalysisCriterionConfig.model_validate(item) for item in criteria],
        target_object_id=target_row_id,
        mode=mode,
        top_n=top_n,
        auto_normalize_weights=True,
        include_explanations=True,
        filter_criteria=AnalysisFilters.model_validate(filter_criteria) if filter_criteria else None,
        include_stability_scenarios=include_stability_scenarios,
        stability_variation_pct=stability_variation_pct,
    )
    result = run_comparative_analysis(payload).model_dump()
    pipeline_logger.info(
        "analyze_dataset_completed rows=%r criteria_count=%r mode=%r top_n=%r ranking_count=%r duration_ms=%.2f",
        len(rows),
        len(criteria),
        mode,
        top_n,
        len(result.get("ranking") or []),
        elapsed_ms(started_at),
    )
    return result


async def fetch_current_user(authorization: str | None) -> dict[str, Any] | None:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", 1)[1]
    try:
        payload = decode_access_token(token)
    except ValueError:
        return None
    with SessionLocal() as db:
        user = get_user(db, int(payload["sub"]))
        if user is None or not user.is_active:
            return None
        return {"id": user.id, "email": user.email, "role": user.role}


async def save_comparison_history(
    *,
    user: dict[str, Any] | None,
    filename: str,
    source_body: bytes | None,
    parameters: dict[str, Any],
    result: dict[str, Any],
    dataset_file_id: int | None = None,
) -> dict[str, Any] | None:
    started_at = start_timer()
    if user is None:
        return None

    adapter = StorageAdapter()
    with SessionLocal() as db:
        if dataset_file_id is None:
            if source_body is None:
                raise ValueError("Source body is required when dataset_file_id is not provided")
            dataset_key, dataset_checksum = adapter.upload(
                filename,
                "application/octet-stream",
                source_body,
                prefix="comparison-dataset",
            )
            dataset_record = create_file_record(
                db,
                original_name=filename,
                content_type="application/octet-stream",
                purpose="comparison-dataset",
                storage_key=dataset_key,
                size_bytes=len(source_body),
                checksum=dataset_checksum,
            )
            dataset_file_id = dataset_record.id

        result_bytes = json.dumps(result, ensure_ascii=False).encode("utf-8")
        result_key, result_checksum = adapter.upload(
            f"{filename}.result.json",
            "application/json",
            result_bytes,
            prefix="comparison-result",
        )
        result_record = create_file_record(
            db,
            original_name=f"{filename}.result.json",
            content_type="application/json",
            purpose="comparison-result",
            storage_key=result_key,
            size_bytes=len(result_bytes),
            checksum=result_checksum,
        )
        history_item = create_comparison_history(
            db,
            ComparisonHistoryCreate(
                user_id=int(user["id"]),
                user_email=str(user["email"]),
                title=str(parameters.get("scenario_title") or f"Сравнение: {filename}"),
                source_filename=filename,
                project_id=parameters.get("project_id"),
                parent_history_id=parameters.get("parent_history_id"),
                dataset_file_id=dataset_file_id,
                result_file_id=result_record.id,
                parameters_json=json.dumps(parameters, ensure_ascii=False),
                summary_json=json.dumps(
                    {
                        "preprocessing_summary": result["preprocessing_summary"],
                        "analysis_summary": result["analysis_summary"],
                        "ranking": result["ranking"],
                    },
                    ensure_ascii=False,
                ),
                tags_json=json.dumps(
                    {
                        "analysis_mode": parameters.get("analysis_mode"),
                        "target_row_id": parameters.get("target_row_id"),
                    },
                    ensure_ascii=False,
                ),
                status="completed",
            ),
        )
        pipeline_logger.info(
            "save_comparison_history_completed user_id=%r history_id=%r dataset_file_id=%r duration_ms=%.2f",
            user.get("id"),
            history_item.id,
            dataset_file_id,
            elapsed_ms(started_at),
        )
        log_audit_event(
            "comparison_history_saved",
            user_id=user.get("id"),
            history_id=history_item.id,
            dataset_file_id=dataset_file_id,
            title=history_item.title,
        )
        return {"id": history_item.id, "project_id": history_item.project_id, "title": history_item.title}


async def fetch_system_dashboard(authorization: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "services": {
            "backend": {"status": "ok", "status_code": 200},
            "database": {"status": "ok", "status_code": 200},
            "storage": {"status": "ok", "status_code": 200},
        },
        "storage": None,
        "auth": None,
        "telemetry": get_telemetry_snapshot(),
    }
    with SessionLocal() as db:
        result["storage"] = storage_stats(db).model_dump()
        current_user = await fetch_current_user(authorization)
        if current_user and current_user.get("role") == "admin":
            result["auth"] = admin_stats(db).model_dump()
    return result


def _prepare_criteria_for_analysis(
    criteria: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    rows: list[dict[str, Any]] | None = None,
    target_row_id: str | None = None,
    fallback_target_values: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    fields_by_key = {field["key"]: field for field in fields}
    target_values: dict[str, Any] = dict(fallback_target_values or {})
    if rows and target_row_id:
        target_row = next((row for row in rows if str(row.get("id")) == str(target_row_id)), None)
        if target_row:
            target_values.update(dict(target_row.get("values") or {}))

    prepared: list[dict[str, Any]] = []
    for criterion in criteria:
        item = dict(criterion)
        field = fields_by_key.get(item["key"])
        if not field:
            prepared.append(item)
            continue
        encoding = field.get("encoding")
        if encoding in {"ordinal", "binary_map"}:
            item["type"] = "numeric"
            item["scale_map"] = None
        if item.get("direction") == "target" and item.get("target_value") is None and target_values:
            item["target_value"] = target_values.get(item["key"])
        if item.get("direction") == "target" and item.get("target_value") is None:
            raise ValueError(
                f"Для критерия '{item.get('name') or item.get('key')}' не удалось определить целевое значение. "
                "Выберите целевой объект с заполненным значением или измените направление критерия."
            )
        prepared.append(item)
    return prepared


def _sanitize_fields_for_preprocessing(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for field in fields:
        item = dict(field)
        if item.get("missing_strategy") == "constant" and item.get("missing_constant") is None:
            item["missing_constant"] = 0 if item.get("field_type") in {"numeric", "integer", "float"} else "unknown"
        if item.get("field_type") in {"numeric", "integer", "float"} and item.get("normalization") in {None, "", "none"}:
            item["normalization"] = "minmax"
        sanitized.append(item)
    return sanitized


async def run_pipeline_via_services(
    *,
    filename: str,
    body: bytes | None,
    payload: PipelineRequest,
    authorization: str | None = None,
    dataset_file_id: int | None = None,
    source_preview: dict[str, Any] | None = None,
) -> PipelineRunResponse:
    total_started_at = start_timer()
    user = await fetch_current_user(authorization)
    preview = source_preview
    if preview is None:
        if body is None:
            raise ValueError("Source dataset body is required when preview artifact is unavailable")
        preview = await fetch_preview(filename=filename, body=body)
    fields_payload = _sanitize_fields_for_preprocessing([field.model_dump() for field in payload.config.fields])
    title_by_id = {row["id"]: _object_title(row) for row in preview["normalized_dataset"]["rows"]}
    raw_target_values: dict[str, Any] = {}
    if payload.config.target_row_id:
        raw_target_row = next(
            (row for row in preview["normalized_dataset"]["rows"] if str(row.get("id")) == str(payload.config.target_row_id)),
            None,
        )
        if raw_target_row:
            raw_target_values = dict(raw_target_row.get("values") or {})

    if dataset_file_id is not None:
        preprocessing, _ = ensure_preprocessed_artifact(
            dataset_file_id=dataset_file_id,
            fields=fields_payload,
            raw_preview=preview,
        )
    else:
        preprocessing = await run_preprocessing(rows=preview["normalized_dataset"]["rows"], fields=fields_payload)
    processed_rows = [
        {
            "id": row["id"],
            "title": title_by_id.get(row["id"], f"Объект {row['id']}"),
            "values": row["values"],
        }
        for row in preprocessing["dataset"]
    ]
    analysis = await analyze_dataset(
        rows=processed_rows,
        criteria=_prepare_criteria_for_analysis(
            [criterion.model_dump() for criterion in payload.config.criteria],
            fields_payload,
            rows=processed_rows,
            target_row_id=payload.config.target_row_id,
            fallback_target_values=raw_target_values,
        ),
        target_row_id=payload.config.target_row_id,
        mode=payload.config.analysis_mode,
        top_n=payload.config.top_n,
        filter_criteria=payload.config.filter_criteria.model_dump() if payload.config.filter_criteria else None,
        include_stability_scenarios=payload.config.include_stability_scenarios,
        stability_variation_pct=payload.config.stability_variation_pct,
    )
    response_payload = PipelineRunResponse(
        import_preview=lightweight_preview(preview),
        preprocessing_summary=preprocessing["summary"],
        analysis_summary=analysis["summary"],
        ranking=analysis["ranking"],
    )
    history_item = await save_comparison_history(
        user=user,
        filename=filename,
        source_body=body,
        parameters=payload.config.model_dump(),
        result=response_payload.model_dump(),
        dataset_file_id=dataset_file_id,
    )
    response_payload.history_id = history_item.get("id") if history_item else None
    pipeline_logger.info(
        "run_pipeline_via_services_completed filename=%r dataset_file_id=%r user_id=%r criteria_count=%r rows_total=%r history_id=%r duration_ms=%.2f",
        filename,
        dataset_file_id,
        user.get("id") if user else None,
        len(payload.config.criteria),
        len(preview["normalized_dataset"]["rows"]),
        response_payload.history_id,
        elapsed_ms(total_started_at),
    )
    return response_payload


async def run_pipeline_from_storage(
    *,
    dataset_file_id: int,
    filename: str | None,
    payload: PipelineRequest,
    authorization: str | None = None,
) -> PipelineRunResponse:
    started_at = start_timer()
    metadata = await fetch_stored_file_metadata(file_id=dataset_file_id)
    resolved_filename = filename or metadata.get("original_name") or payload.filename or "dataset"
    raw_preview = ensure_raw_preview_artifact(dataset_file_id=dataset_file_id, filename=resolved_filename)
    response = await run_pipeline_via_services(
        filename=resolved_filename,
        body=None,
        payload=payload,
        authorization=authorization,
        dataset_file_id=dataset_file_id,
        source_preview=raw_preview,
    )
    pipeline_logger.info(
        "run_pipeline_from_storage_completed dataset_file_id=%r filename=%r history_id=%r duration_ms=%.2f",
        dataset_file_id,
        resolved_filename,
        response.history_id,
        elapsed_ms(started_at),
    )
    return response


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _build_preview_from_processed(
    *,
    filename: str,
    rows: list[dict[str, Any]],
    profile: dict[str, Any],
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    profile_fields = list(profile.get("fields") or [])
    profile_by_key = {str(item.get("key")): item for item in profile_fields if item.get("key")}
    ordered_keys = [str(item.get("key")) for item in profile_fields if item.get("key")]
    if not ordered_keys and rows:
        ordered_keys = list((rows[0].get("values") or {}).keys())

    normalized_rows = [{"id": str(row.get("id", "")), "values": dict(row.get("values") or {})} for row in rows]
    columns: list[dict[str, Any]] = []
    for key in ordered_keys:
        values = [row["values"].get(key) for row in normalized_rows]
        non_missing = [value for value in values if not _is_missing(value)]
        field_profile = profile_by_key.get(key) or {}
        columns.append(
            {
                "source_name": key,
                "normalized_name": key,
                "inferred_type": field_profile.get("inferred_type", "text"),
                "missing_count": len(values) - len(non_missing),
                "unique_count": len({str(value) for value in non_missing}),
                "sample_values": non_missing[:5],
            }
        )

    preview_rows = [{"id": row["id"], **row["values"]} for row in normalized_rows[:50]]
    return {
        "filename": filename,
        "rows_total": len(normalized_rows),
        "columns": columns,
        "preview_rows": preview_rows,
        "warnings": warnings or [],
        "normalized_dataset": {"rows": normalized_rows},
    }


async def refresh_preprocessing_from_storage(
    *,
    dataset_file_id: int,
    filename: str | None,
    fields: list[dict[str, Any]],
    histogram_bins: int = 8,
    histogram_bins_by_field: dict[str, int] | None = None,
    detail_level: ProfileDetailLevel = "detailed",
) -> dict[str, Any]:
    total_started_at = start_timer()
    raw_preview = ensure_raw_preview_artifact(dataset_file_id=dataset_file_id, filename=filename)
    resolved_filename = filename or raw_preview.get("filename") or "dataset"
    sanitized_fields = _sanitize_fields_for_preprocessing(fields)
    preprocessing, fields_signature = ensure_preprocessed_artifact(
        dataset_file_id=dataset_file_id,
        fields=sanitized_fields,
        raw_preview=raw_preview,
    )
    profile = ensure_preprocessed_profile_artifact(
        dataset_file_id=dataset_file_id,
        fields_signature=fields_signature,
        rows=preprocessing["dataset"],
        detail_level=detail_level,
        histogram_bins=histogram_bins,
        histogram_bins_by_field=histogram_bins_by_field,
    )
    refreshed_preview = _build_preview_from_processed(
        filename=resolved_filename,
        rows=preprocessing["dataset"],
        profile=profile,
        warnings=list(raw_preview.get("warnings") or []),
    )
    result = {
        "preview": lightweight_preview(refreshed_preview),
        "profile": profile,
        "preprocessing_summary": preprocessing.get("summary") or {},
    }
    pipeline_logger.info(
        "refresh_preprocessing_from_storage_completed dataset_file_id=%r filename=%r fields_count=%r rows_total=%r duration_ms=%.2f",
        dataset_file_id,
        resolved_filename,
        len(fields),
        len(preprocessing.get("dataset") or []),
        elapsed_ms(total_started_at),
    )
    return result
