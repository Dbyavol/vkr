from __future__ import annotations

import base64
import json
from typing import Any

import httpx

from app.core_config import Settings
from app.schemas.pipeline import PipelineRequest, PipelineRunResponse


PREVIEW_NORMALIZED_ROW_LIMIT = 200


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
                "id": row["id"],
                "title": _object_title(row),
                "attributes": row["values"],
            }
            for row in rows
        ]
    }


async def fetch_preview(
    *,
    settings: Settings,
    filename: str,
    body: bytes,
) -> dict[str, Any]:
    payload = {
        "filename": filename,
        "content_base64": base64.b64encode(body).decode("utf-8"),
    }
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.post(f"{settings.import_service_url}/api/v1/imports/parse-base64", json=payload)
        response.raise_for_status()
        return response.json()


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


async def upload_dataset_file(
    *,
    settings: Settings,
    filename: str,
    body: bytes,
) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.post(
            f"{settings.storage_service_url}/api/v1/files/upload",
            params={"purpose": "comparison-dataset-source"},
            files={"file": (filename, body, "application/octet-stream")},
        )
        response.raise_for_status()
        return response.json()


async def fetch_stored_file_metadata(*, settings: Settings, file_id: int) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.get(f"{settings.storage_service_url}/api/v1/files/{file_id}")
        response.raise_for_status()
        return response.json()


async def fetch_stored_file_body(*, settings: Settings, file_id: int) -> bytes:
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.get(f"{settings.storage_service_url}/api/v1/files/{file_id}/content")
        response.raise_for_status()
        return response.content


async def preprocess_dataset(
    *,
    settings: Settings,
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "dataset": {"rows": rows},
        "fields": fields,
        "options": {
            "drop_duplicate_rows": True,
            "duplicate_keys": [],
            "keep_columns_not_in_config": False,
            "preserve_original_values": False,
        },
    }
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.post(
            f"{settings.preprocessing_service_url}/api/v1/preprocessing/run",
            json=payload,
        )
        response.raise_for_status()
        return response.json()


async def profile_imported_dataset(
    *,
    settings: Settings,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "dataset": {"rows": rows},
        "max_unique_values": 30,
        "histogram_bins": 8,
    }
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.post(
            f"{settings.preprocessing_service_url}/api/v1/preprocessing/profile",
            json=payload,
        )
        response.raise_for_status()
        return response.json()


async def fetch_dataset_profile(
    *,
    settings: Settings,
    filename: str,
    body: bytes,
) -> dict[str, Any]:
    preview = await fetch_preview(settings=settings, filename=filename, body=body)
    profile = await profile_imported_dataset(settings=settings, rows=preview["normalized_dataset"]["rows"])
    return {"preview": lightweight_preview(preview), "profile": profile}


async def upload_and_profile_dataset(
    *,
    settings: Settings,
    filename: str,
    body: bytes,
) -> dict[str, Any]:
    uploaded = await upload_dataset_file(settings=settings, filename=filename, body=body)
    profiled = await fetch_dataset_profile(settings=settings, filename=filename, body=body)
    return {"dataset_file_id": uploaded["id"], **profiled}


async def analyze_dataset(
    *,
    settings: Settings,
    rows: list[dict[str, Any]],
    criteria: list[dict[str, Any]],
    target_row_id: str | None,
    mode: str,
    top_n: int,
    filter_criteria: dict[str, Any] | None,
    include_stability_scenarios: bool,
    stability_variation_pct: float,
) -> dict[str, Any]:
    payload = {
        "dataset": _analysis_dataset(rows),
        "criteria": criteria,
        "target_object_id": target_row_id,
        "mode": mode,
        "top_n": top_n,
        "auto_normalize_weights": True,
        "include_explanations": True,
        "filter_criteria": filter_criteria,
        "include_stability_scenarios": include_stability_scenarios,
        "stability_variation_pct": stability_variation_pct,
    }
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.post(
            f"{settings.analysis_service_url}/api/v1/analysis/run",
            json=payload,
        )
        response.raise_for_status()
        return response.json()


async def fetch_current_user(settings: Settings, authorization: str | None) -> dict[str, Any] | None:
    if not authorization:
        return None
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.get(
            f"{settings.auth_service_url}/api/v1/users/me",
            headers={"Authorization": authorization},
        )
        response.raise_for_status()
        return response.json()


async def save_comparison_history(
    *,
    settings: Settings,
    user: dict[str, Any] | None,
    filename: str,
    source_body: bytes | None,
    parameters: dict[str, Any],
    result: dict[str, Any],
    dataset_file_id: int | None = None,
) -> dict[str, Any] | None:
    if user is None:
        return None
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        dataset_file = {"id": dataset_file_id}
        if dataset_file_id is None:
            if source_body is None:
                raise ValueError("Source body is required when dataset_file_id is not provided")
            dataset_upload = await client.post(
                f"{settings.storage_service_url}/api/v1/files/upload",
                params={"purpose": "comparison-dataset"},
                files={"file": (filename, source_body, "application/octet-stream")},
            )
            dataset_upload.raise_for_status()
            dataset_file = dataset_upload.json()
        result_upload = await client.post(
            f"{settings.storage_service_url}/api/v1/files/upload",
            params={"purpose": "comparison-result"},
            files={
                "file": (
                    f"{filename}.result.json",
                    json.dumps(result, ensure_ascii=False).encode("utf-8"),
                    "application/json",
                )
            },
        )
        result_upload.raise_for_status()
        result_file = result_upload.json()
    payload = {
        "user_id": user["id"],
        "user_email": user["email"],
        "title": f"Сравнение: {filename}",
        "project_id": parameters.get("project_id"),
        "parent_history_id": parameters.get("parent_history_id"),
        "source_filename": filename,
        "dataset_file_id": dataset_file["id"],
        "result_file_id": result_file["id"],
        "parameters_json": json.dumps(parameters, ensure_ascii=False),
        "summary_json": json.dumps(
            {
                "preprocessing_summary": result["preprocessing_summary"],
                "analysis_summary": result["analysis_summary"],
                "ranking": result["ranking"],
            },
            ensure_ascii=False,
        ),
        "tags_json": json.dumps(
            {
                "analysis_mode": parameters.get("analysis_mode"),
                "target_row_id": parameters.get("target_row_id"),
            },
            ensure_ascii=False,
        ),
        "status": "completed",
    }
    if parameters.get("scenario_title"):
        payload["title"] = str(parameters["scenario_title"])
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        response = await client.post(f"{settings.storage_service_url}/api/v1/comparison-history", json=payload)
        response.raise_for_status()
        return response.json()


async def fetch_system_dashboard(settings: Settings, authorization: str | None = None) -> dict[str, Any]:
    services = {
        "auth": settings.auth_service_url,
        "import": settings.import_service_url,
        "preprocessing": settings.preprocessing_service_url,
        "analysis": settings.analysis_service_url,
        "storage": settings.storage_service_url,
    }
    result: dict[str, Any] = {"services": {}, "storage": None, "auth": None}
    async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
        for name, url in services.items():
            try:
                response = await client.get(f"{url}/health")
                result["services"][name] = {
                    "status": "ok" if response.is_success else "error",
                    "status_code": response.status_code,
                }
            except httpx.HTTPError as exc:
                result["services"][name] = {"status": "error", "message": str(exc)}
        try:
            storage = await client.get(f"{settings.storage_service_url}/api/v1/stats")
            if storage.is_success:
                result["storage"] = storage.json()
        except httpx.HTTPError as exc:
            result["storage"] = {"error": str(exc)}
        if authorization:
            try:
                auth = await client.get(
                    f"{settings.auth_service_url}/api/v1/admin/stats",
                    headers={"Authorization": authorization},
                )
                if auth.is_success:
                    result["auth"] = auth.json()
            except httpx.HTTPError as exc:
                result["auth"] = {"error": str(exc)}
    return result


def _prepare_criteria_for_analysis(
    criteria: list[dict[str, Any]],
    fields: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    fields_by_key = {field["key"]: field for field in fields}
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
        prepared.append(item)
    return prepared


async def run_pipeline_via_services(
    *,
    settings: Settings,
    filename: str,
    body: bytes,
    payload: PipelineRequest,
    authorization: str | None = None,
    dataset_file_id: int | None = None,
) -> PipelineRunResponse:
    user = await fetch_current_user(settings, authorization)
    preview = await fetch_preview(settings=settings, filename=filename, body=body)
    title_by_id = {
        row["id"]: _object_title(row)
        for row in preview["normalized_dataset"]["rows"]
    }
    preprocessing = await preprocess_dataset(
        settings=settings,
        rows=preview["normalized_dataset"]["rows"],
        fields=[field.model_dump() for field in payload.config.fields],
    )
    processed_rows = [
        {
            "id": row["id"],
            "title": title_by_id.get(row["id"], f"Объект {row['id']}"),
            "values": row["values"],
        }
        for row in preprocessing["dataset"]
    ]
    analysis = await analyze_dataset(
        settings=settings,
        rows=processed_rows,
        criteria=_prepare_criteria_for_analysis(
            [criterion.model_dump() for criterion in payload.config.criteria],
            [field.model_dump() for field in payload.config.fields],
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
        settings=settings,
        user=user,
        filename=filename,
        source_body=body,
        parameters=payload.config.model_dump(),
        result=response_payload.model_dump(),
        dataset_file_id=dataset_file_id,
    )
    response_payload.history_id = history_item.get("id") if history_item else None
    return response_payload


async def run_pipeline_from_storage(
    *,
    settings: Settings,
    dataset_file_id: int,
    filename: str | None,
    payload: PipelineRequest,
    authorization: str | None = None,
) -> PipelineRunResponse:
    metadata = await fetch_stored_file_metadata(settings=settings, file_id=dataset_file_id)
    body = await fetch_stored_file_body(settings=settings, file_id=dataset_file_id)
    resolved_filename = filename or metadata.get("original_name") or payload.filename or "dataset"
    return await run_pipeline_via_services(
        settings=settings,
        filename=resolved_filename,
        body=body,
        payload=payload,
        authorization=authorization,
        dataset_file_id=dataset_file_id,
    )


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

    normalized_rows = [
        {
            "id": str(row.get("id", "")),
            "values": dict(row.get("values") or {}),
        }
        for row in rows
    ]

    columns: list[dict[str, Any]] = []
    for key in ordered_keys:
        values = [row["values"].get(key) for row in normalized_rows]
        non_missing = [value for value in values if not _is_missing(value)]
        unique_count = len({str(value) for value in non_missing})
        sample_values = non_missing[:5]
        field_profile = profile_by_key.get(key) or {}
        columns.append(
            {
                "source_name": key,
                "normalized_name": key,
                "inferred_type": field_profile.get("inferred_type", "text"),
                "missing_count": len(values) - len(non_missing),
                "unique_count": unique_count,
                "sample_values": sample_values,
            }
        )

    preview_rows = []
    for row in normalized_rows[:50]:
        preview_rows.append({"id": row["id"], **row["values"]})

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
    settings: Settings,
    dataset_file_id: int,
    filename: str | None,
    fields: list[dict[str, Any]],
) -> dict[str, Any]:
    metadata = await fetch_stored_file_metadata(settings=settings, file_id=dataset_file_id)
    body = await fetch_stored_file_body(settings=settings, file_id=dataset_file_id)
    resolved_filename = filename or metadata.get("original_name") or "dataset"
    import_preview = await fetch_preview(settings=settings, filename=resolved_filename, body=body)
    preprocessing = await preprocess_dataset(
        settings=settings,
        rows=import_preview["normalized_dataset"]["rows"],
        fields=fields,
    )
    profile = await profile_imported_dataset(settings=settings, rows=preprocessing["dataset"])
    refreshed_preview = _build_preview_from_processed(
        filename=resolved_filename,
        rows=preprocessing["dataset"],
        profile=profile,
        warnings=list(import_preview.get("warnings") or []),
    )
    return {
        "preview": lightweight_preview(refreshed_preview),
        "profile": profile,
        "preprocessing_summary": preprocessing.get("summary") or {},
    }
