from __future__ import annotations

import base64
import csv
import io
import json
import re
from typing import Any

import pandas as pd

from app.services.measurement_parsing import parse_measurement
from app.schemas.imports import ColumnInfo, ImportCommitRequest, ImportCommitResponse, ImportPreviewResponse


def _normalize_column(name: str) -> str:
    normalized = name.strip().lower()
    normalized = normalized.replace(" ", "_")
    normalized = re.sub(r"[^a-zA-Z0-9_а-яА-Я]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "column"


def _coerce_nan(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _infer_type(values: list[Any]) -> str:
    non_null = [value for value in values if value is not None and value != ""]
    if not non_null:
        return "unknown"
    if all(isinstance(value, bool) for value in non_null):
        return "binary"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null):
        return "numeric"
    numeric_values = [parsed.value for parsed in map(parse_measurement, non_null) if parsed.value is not None]
    if len(numeric_values) == len(non_null):
        return "numeric"
    if len(set(map(str, non_null))) <= 2 and all(str(value).lower() in {"true", "false", "0", "1"} for value in non_null):
        return "binary"
    return "categorical" if len(set(map(str, non_null))) <= max(10, len(non_null) // 5) else "text"


def _read_json_to_frame(body: bytes) -> pd.DataFrame:
    loaded = json.loads(body.decode("utf-8-sig"))
    if isinstance(loaded, dict):
        if "rows" in loaded and isinstance(loaded["rows"], list):
            return pd.DataFrame(list(loaded["rows"]))
        raise ValueError("JSON object must contain a top-level 'rows' array or be an array itself")
    if isinstance(loaded, list):
        return pd.DataFrame(loaded)
    raise ValueError("Unsupported JSON structure")


def _read_xlsx_to_frame(body: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(body), dtype=object)


def _detect_csv_text_and_delimiter(body: bytes) -> tuple[str, str]:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError:
        # Many Russian public datasets are exported in cp1251.
        text = body.decode("cp1251")

    sample = text[:4096]
    delimiters = [",", ";", "\t", "|"]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=delimiters)
        delimiter = dialect.delimiter
    except csv.Error:
        first_line = text.splitlines()[0] if text else ""
        delimiter = max(delimiters, key=first_line.count)
    return text, delimiter


def _read_csv_to_frame(body: bytes) -> pd.DataFrame:
    text, delimiter = _detect_csv_text_and_delimiter(body)
    return pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        dtype=object,
        keep_default_na=False,
    )


def _frame_from_bytes(filename: str, body: bytes) -> pd.DataFrame:
    lower_name = filename.lower()
    if lower_name.endswith(".json"):
        frame = _read_json_to_frame(body)
    elif lower_name.endswith(".xlsx"):
        frame = _read_xlsx_to_frame(body)
    elif lower_name.endswith(".csv"):
        frame = _read_csv_to_frame(body)
    else:
        raise ValueError("Supported file types: CSV, XLSX, JSON")

    frame.columns = [str(column) for column in frame.columns]
    return frame


def _normalize_frame_columns(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[ColumnInfo], list[str]]:
    source_columns = list(frame.columns)
    normalized_map: dict[str, str] = {}
    used_names: set[str] = set()
    warnings: list[str] = []

    for column in source_columns:
        base_name = _normalize_column(str(column))
        normalized = base_name
        index = 2
        while normalized in used_names:
            normalized = f"{base_name}_{index}"
            index += 1
        if normalized != column:
            warnings.append(f"Column '{column}' normalized to '{normalized}'")
        normalized_map[str(column)] = normalized
        used_names.add(normalized)

    normalized_frame = frame.rename(columns=normalized_map).copy()
    normalized_frame = normalized_frame.where(pd.notna(normalized_frame), None)

    columns: list[ColumnInfo] = []
    for source_column in source_columns:
        normalized_name = normalized_map[str(source_column)]
        series = normalized_frame[normalized_name]
        values = [_coerce_nan(value) for value in series.tolist()]
        unique_non_null = {
            json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list)) else str(value)
            for value in values
            if value is not None
        }
        columns.append(
            ColumnInfo(
                source_name=str(source_column),
                normalized_name=normalized_name,
                inferred_type=_infer_type(values),
                missing_count=sum(1 for value in values if value is None or value == ""),
                unique_count=len(unique_non_null),
                sample_values=values[:5],
            )
        )
    return normalized_frame, columns, warnings


def _rows_from_frame(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_rows: list[dict[str, Any]] = []
    preview_rows: list[dict[str, Any]] = []
    for index, record in enumerate(frame.to_dict(orient="records"), start=1):
        normalized_values = {str(key): _coerce_nan(value) for key, value in record.items()}
        normalized_rows.append({"id": str(index), "values": normalized_values})
        if index <= 5:
            preview_rows.append(normalized_values)
    return normalized_rows, preview_rows


def parse_dataset_bytes(filename: str, body: bytes) -> ImportPreviewResponse:
    frame = _frame_from_bytes(filename, body)
    if frame.empty:
        raise ValueError("Input file contains no rows")

    normalized_frame, columns, warnings = _normalize_frame_columns(frame)
    normalized_rows, preview_rows = _rows_from_frame(normalized_frame)

    return ImportPreviewResponse(
        filename=filename,
        rows_total=len(normalized_rows),
        columns=columns,
        preview_rows=preview_rows,
        warnings=warnings,
        normalized_dataset={"rows": normalized_rows},
    )


def parse_dataset_base64(filename: str, content_base64: str) -> ImportPreviewResponse:
    body = base64.b64decode(content_base64)
    return parse_dataset_bytes(filename, body)


def build_commit_response(payload: ImportCommitRequest) -> ImportCommitResponse:
    handoff_payload = {
        "dataset": {
            "rows": [row.model_dump() for row in payload.dataset.rows],
        },
        "source": {
            "filename": payload.source_filename,
            "dataset_name": payload.dataset_name,
            "object_type_code": payload.object_type_code,
            "storage_file_id": payload.storage_file_id,
        },
        "schema_hint": payload.schema_hint or {},
    }
    return ImportCommitResponse(
        dataset_name=payload.dataset_name,
        rows_total=len(payload.dataset.rows),
        status="ready_for_preprocessing",
        handoff_payload=handoff_payload,
    )
