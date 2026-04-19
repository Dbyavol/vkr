from __future__ import annotations

import base64
import json
from typing import Any

import httpx

from app.core_config import Settings
from app.schemas.pipeline import PipelineRequest, PipelineRunResponse


def _analysis_dataset(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "objects": [
            {
                "id": row["id"],
                "title": f"Object {row['id']}",
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
    return {"preview": preview, "profile": profile}


async def analyze_dataset(
    *,
    settings: Settings,
    rows: list[dict[str, Any]],
    criteria: list[dict[str, Any]],
    target_row_id: str | None,
    mode: str,
    top_n: int,
) -> dict[str, Any]:
    payload = {
        "dataset": _analysis_dataset(rows),
        "criteria": criteria,
        "target_object_id": target_row_id,
        "mode": mode,
        "top_n": top_n,
        "auto_normalize_weights": True,
        "include_explanations": True,
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
    source_body: bytes,
    parameters: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any] | None:
    if user is None:
        return None
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, trust_env=False) as client:
        dataset_upload = await client.post(
            f"{settings.storage_service_url}/api/v1/files/upload",
            params={"purpose": "comparison-dataset"},
            files={"file": (filename, source_body, "application/octet-stream")},
        )
        dataset_upload.raise_for_status()
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
        dataset_file = dataset_upload.json()
        result_file = result_upload.json()
    payload = {
        "user_id": user["id"],
        "user_email": user["email"],
        "title": f"Comparison: {filename}",
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
) -> PipelineRunResponse:
    user = await fetch_current_user(settings, authorization)
    preview = await fetch_preview(settings=settings, filename=filename, body=body)
    preprocessing = await preprocess_dataset(
        settings=settings,
        rows=preview["normalized_dataset"]["rows"],
        fields=[field.model_dump() for field in payload.config.fields],
    )
    processed_rows = [
        {
            "id": row["id"],
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
    )
    response_payload = PipelineRunResponse(
        import_preview=preview,
        preprocessing_summary=preprocessing["summary"],
        analysis_summary=analysis["summary"],
        ranking=analysis["ranking"],
    )
    await save_comparison_history(
        settings=settings,
        user=user,
        filename=filename,
        source_body=body,
        parameters=payload.config.model_dump(),
        result=response_payload.model_dump(),
    )
    return response_payload
