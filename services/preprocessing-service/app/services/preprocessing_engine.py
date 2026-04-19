from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime
from statistics import median
from typing import Any

from app.schemas.preprocessing import (
    FieldConfig,
    FieldReport,
    PreprocessingRequest,
    PreprocessingResponse,
    PreprocessingSummary,
    ProcessedRow,
)


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        if isinstance(value, str) and value.strip():
            return float(value)
    except ValueError:
        return None
    return None


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    formats = (
        None,
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d.%m.%Y",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
    )
    for fmt in formats:
        try:
            if fmt is None:
                return datetime.fromisoformat(normalized)
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    index = (len(sorted_values) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[int(index)]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + (upper_value - lower_value) * (index - lower)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: list[float]) -> float:
    if not values:
        return 0.0
    avg = _mean(values)
    variance = sum((item - avg) ** 2 for item in values) / len(values)
    return math.sqrt(variance)


def _mode(values: list[Any]) -> Any:
    counts: dict[Any, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))[0][0]


def _numeric_series(rows: list[dict], key: str) -> list[float]:
    result = []
    for row in rows:
        value = _to_float(row["values"].get(key))
        if value is not None:
            result.append(value)
    return result


def _categorical_series(rows: list[dict], key: str) -> list[Any]:
    return [row["values"].get(key) for row in rows if not _is_missing(row["values"].get(key))]


def _fill_missing(rows: list[dict], config: FieldConfig, report: FieldReport) -> tuple[list[dict], int]:
    rows_removed = 0
    if config.missing_strategy == "none":
        return rows, rows_removed

    fill_value = None
    if config.missing_strategy == "mean":
        values = _numeric_series(rows, config.key)
        fill_value = _mean(values) if values else None
    elif config.missing_strategy == "median":
        values = _numeric_series(rows, config.key)
        fill_value = median(values) if values else None
    elif config.missing_strategy == "mode":
        fill_value = _mode(_categorical_series(rows, config.key))
    elif config.missing_strategy == "constant":
        fill_value = config.missing_constant

    processed: list[dict] = []
    for row in rows:
        value = row["values"].get(config.key)
        if _is_missing(value):
            if config.missing_strategy == "drop_row":
                rows_removed += 1
                continue
            row["values"][config.key] = fill_value
        processed.append(row)

    if fill_value is not None and config.missing_strategy != "drop_row":
        report.notes.append(f"Missing values filled using {config.missing_strategy}")
    if rows_removed:
        report.notes.append("Rows with missing values were removed")
    return processed, rows_removed


def _outlier_bounds(values: list[float], method: str, threshold: float) -> tuple[float, float]:
    sorted_values = sorted(values)
    if method.startswith("iqr"):
        q1 = _percentile(sorted_values, 0.25)
        q3 = _percentile(sorted_values, 0.75)
        iqr = q3 - q1
        return q1 - threshold * iqr, q3 + threshold * iqr
    avg = _mean(sorted_values)
    std = _std(sorted_values)
    if std == 0:
        return avg, avg
    return avg - threshold * std, avg + threshold * std


def _handle_outliers(rows: list[dict], config: FieldConfig, report: FieldReport) -> tuple[list[dict], int, int]:
    if config.outlier_method == "none" or config.field_type != "numeric":
        return rows, 0, 0

    values = _numeric_series(rows, config.key)
    if len(values) < 2:
        report.notes.append("Outlier handling skipped due to insufficient numeric values")
        return rows, 0, 0

    low, high = _outlier_bounds(values, config.outlier_method, config.outlier_threshold)
    removed = 0
    clipped = 0
    processed: list[dict] = []

    for row in rows:
        numeric = _to_float(row["values"].get(config.key))
        if numeric is None:
            processed.append(row)
            continue

        if numeric < low or numeric > high:
            if config.outlier_method.endswith("remove"):
                removed += 1
                continue
            row["values"][config.key] = max(low, min(high, numeric))
            clipped += 1
        processed.append(row)

    if removed:
        report.notes.append(f"Outliers removed using {config.outlier_method}")
    if clipped:
        report.notes.append(f"Outliers clipped using {config.outlier_method}")
    return processed, removed, clipped


def _normalize_value(value: float, method: str, values: list[float]) -> float:
    if method == "none":
        return value
    if not values:
        return value
    min_value = min(values)
    max_value = max(values)
    avg = _mean(values)
    std = _std(values)
    sorted_values = sorted(values)

    if method == "minmax":
        return 1.0 if min_value == max_value else (value - min_value) / (max_value - min_value)
    if method == "zscore":
        return 0.0 if std == 0 else (value - avg) / std
    if method == "robust":
        q1 = _percentile(sorted_values, 0.25)
        q3 = _percentile(sorted_values, 0.75)
        iqr = q3 - q1
        med = median(sorted_values)
        return 0.0 if iqr == 0 else (value - med) / iqr
    if method == "log_minmax":
        safe_values = [math.log1p(max(item, 0.0)) for item in values]
        safe_value = math.log1p(max(value, 0.0))
        safe_min = min(safe_values)
        safe_max = max(safe_values)
        return 1.0 if safe_min == safe_max else (safe_value - safe_min) / (safe_max - safe_min)
    return value


def _encode_rows(rows: list[dict], config: FieldConfig, report: FieldReport) -> tuple[list[dict], list[str]]:
    generated_columns: list[str] = []

    if config.encoding == "none":
        return rows, generated_columns

    if config.encoding == "ordinal":
        mapping = config.ordinal_map or {}
        for row in rows:
            row["values"][config.key] = mapping.get(str(row["values"].get(config.key)), 0.0)
        report.notes.append("Ordinal encoding applied")
        return rows, generated_columns

    if config.encoding == "binary_map":
        mapping = config.binary_map or {}
        for row in rows:
            row["values"][config.key] = mapping.get(str(row["values"].get(config.key)).lower(), 0.0)
        report.notes.append("Binary mapping applied")
        return rows, generated_columns

    if config.encoding == "one_hot":
        categories = sorted(
            {str(row["values"].get(config.key)) for row in rows if not _is_missing(row["values"].get(config.key))}
        )
        for category in categories:
            column = f"{config.key}__{category}"
            generated_columns.append(column)
            for row in rows:
                row["values"][column] = 1.0 if str(row["values"].get(config.key)) == category else 0.0
        report.notes.append("One-hot encoding applied")
        return rows, generated_columns

    return rows, generated_columns


def _derive_datetime_features(rows: list[dict], config: FieldConfig, report: FieldReport) -> tuple[list[dict], list[str]]:
    generated_columns: list[str] = []
    if config.field_type != "datetime":
        return rows, generated_columns

    derived_map = {
        f"{config.key}__year": lambda dt: float(dt.year),
        f"{config.key}__month": lambda dt: float(dt.month),
        f"{config.key}__day": lambda dt: float(dt.day),
        f"{config.key}__day_of_week": lambda dt: float(dt.weekday()),
    }
    generated_columns = list(derived_map)
    success_count = 0

    for row in rows:
        parsed = _to_datetime(row["values"].get(config.key))
        if parsed is None:
            for column in generated_columns:
                row["values"][column] = None
            continue
        success_count += 1
        for column, extractor in derived_map.items():
            row["values"][column] = extractor(parsed)

    if success_count:
        report.notes.append("Datetime features generated: year, month, day, day_of_week")
    else:
        report.notes.append("Datetime feature generation skipped: no parseable values")
    return rows, generated_columns


def _normalize_rows(rows: list[dict], config: FieldConfig, report: FieldReport) -> list[dict]:
    if config.normalization == "none" or config.field_type != "numeric":
        return rows
    values = _numeric_series(rows, config.key)
    if not values:
        report.notes.append("Normalization skipped due to missing numeric values")
        return rows
    for row in rows:
        numeric = _to_float(row["values"].get(config.key))
        if numeric is None:
            continue
        row["values"][config.key] = round(_normalize_value(numeric, config.normalization, values), 6)
    report.notes.append(f"Normalization applied: {config.normalization}")
    return rows


def _drop_duplicates(rows: list[dict], duplicate_keys: list[str]) -> tuple[list[dict], int]:
    if not rows:
        return rows, 0
    seen: set[tuple] = set()
    unique_rows: list[dict] = []
    removed = 0
    for row in rows:
        if duplicate_keys:
            signature = tuple(row["values"].get(key) for key in duplicate_keys)
        else:
            signature = tuple(sorted(row["values"].items()))
        if signature in seen:
            removed += 1
            continue
        seen.add(signature)
        unique_rows.append(row)
    return unique_rows, removed


def preprocess_dataset(payload: PreprocessingRequest) -> PreprocessingResponse:
    rows = [
        {"id": row.id, "values": deepcopy(row.values), "original_values": deepcopy(row.values)}
        for row in payload.dataset.rows
    ]

    removed_duplicates = 0
    if payload.options.drop_duplicate_rows:
        rows, removed_duplicates = _drop_duplicates(rows, payload.options.duplicate_keys)

    field_reports: list[FieldReport] = []
    total_removed_missing = 0
    total_removed_outliers = 0
    total_clipped = 0
    generated_columns: list[str] = []
    field_configs = {field.key: field for field in payload.fields}

    for config in payload.fields:
        report = FieldReport(
            key=config.key,
            field_type=config.field_type,
            rows_missing_before=sum(1 for row in rows if _is_missing(row["values"].get(config.key))),
            rows_missing_after=0,
            rows_removed_as_outliers=0,
            values_clipped=0,
            encoding_applied=config.encoding,
            normalization_applied=config.normalization,
            notes=[],
        )

        rows, removed_missing = _fill_missing(rows, config, report)
        rows, removed_outliers, clipped = _handle_outliers(rows, config, report)
        rows, datetime_columns = _derive_datetime_features(rows, config, report)
        rows, new_columns = _encode_rows(rows, config, report)
        rows = _normalize_rows(rows, config, report)

        report.rows_missing_after = sum(1 for row in rows if _is_missing(row["values"].get(config.key)))
        report.rows_removed_as_outliers = removed_outliers
        report.values_clipped = clipped
        total_removed_missing += removed_missing
        total_removed_outliers += removed_outliers
        total_clipped += clipped
        generated_columns.extend(datetime_columns)
        generated_columns.extend(new_columns)
        field_reports.append(report)

    output_rows: list[ProcessedRow] = []
    for row in rows:
        if payload.options.keep_columns_not_in_config:
            values = row["values"]
        else:
            values = {
                key: value
                for key, value in row["values"].items()
                if key in field_configs or key in generated_columns
            }
        for config in payload.fields:
            if not config.include_in_output and config.key in values:
                values.pop(config.key, None)
        output_rows.append(
            ProcessedRow(
                id=row["id"],
                values=values,
                original_values=row["original_values"] if payload.options.preserve_original_values else None,
            )
        )

    return PreprocessingResponse(
        summary=PreprocessingSummary(
            rows_input=len(payload.dataset.rows),
            rows_output=len(output_rows),
            rows_removed_duplicates=removed_duplicates,
            rows_removed_missing=total_removed_missing,
            rows_removed_outliers=total_removed_outliers,
            generated_columns=generated_columns,
        ),
        field_reports=field_reports,
        dataset=output_rows,
    )
