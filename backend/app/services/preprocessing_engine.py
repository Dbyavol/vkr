from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from app.services.measurement_parsing import coerce_measurement_series
from app.schemas.preprocessing import (
    FieldConfig,
    FieldReport,
    PreprocessingRequest,
    PreprocessingResponse,
    PreprocessingSummary,
    ProcessedRow,
)


DATETIME_FORMAT_ALIASES: dict[str, str] = {
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYY/MM/DD": "%Y/%m/%d",
    "DD.MM.YYYY": "%d.%m.%Y",
    "DD-MM-YYYY": "%d-%m-%Y",
    "YYYY-MM-DD HH:mm:ss": "%Y-%m-%d %H:%M:%S",
    "YYYY/MM/DD HH:mm:ss": "%Y/%m/%d %H:%M:%S",
    "DD.MM.YYYY HH:mm:ss": "%d.%m.%Y %H:%M:%S",
}


def _to_datetime(value: Any, preferred_format: str | None = None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    preferred_python_format = DATETIME_FORMAT_ALIASES.get(preferred_format or "")
    formats = []
    if preferred_python_format:
        formats.append(preferred_python_format)
    formats.extend(
        [
            None,
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d.%m.%Y",
            "%d-%m-%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
        ]
    )
    for fmt in formats:
        try:
            if fmt is None:
                return datetime.fromisoformat(normalized)
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _as_numeric(series: pd.Series) -> pd.Series:
    normalized = series.replace({True: 1.0, False: 0.0})
    return coerce_measurement_series(normalized, field_key=series.name or "value").series


def _missing_mask(series: pd.Series) -> pd.Series:
    return series.isna() | series.eq("")


def _normalize_numeric(series: pd.Series, method: str) -> pd.Series:
    valid = series.dropna()
    if valid.empty or method == "none":
        return series

    if method == "minmax":
        min_value = valid.min()
        max_value = valid.max()
        if min_value == max_value:
            return series.where(series.isna(), 1.0)
        return (series - min_value) / (max_value - min_value)

    if method == "zscore":
        avg = valid.mean()
        std = valid.std(ddof=0)
        if std == 0 or pd.isna(std):
            return series.where(series.isna(), 0.0)
        return (series - avg) / std

    if method == "robust":
        q1 = valid.quantile(0.25)
        q3 = valid.quantile(0.75)
        iqr = q3 - q1
        med = valid.median()
        if iqr == 0 or pd.isna(iqr):
            return series.where(series.isna(), 0.0)
        return (series - med) / iqr

    if method == "log_minmax":
        safe = series.clip(lower=0)
        safe_valid = valid.clip(lower=0)
        logged = np.log1p(safe)
        logged_valid = np.log1p(safe_valid)
        min_value = logged_valid.min()
        max_value = logged_valid.max()
        if min_value == max_value:
            return logged.where(series.isna(), 1.0)
        return (logged - min_value) / (max_value - min_value)

    return series


def _is_numeric_field(field_type: str) -> bool:
    return field_type in {"numeric", "integer", "float", "geo_latitude", "geo_longitude"}


def preprocess_rows(
    rows_input: list[dict[str, Any]],
    fields_input: list[dict[str, Any]],
    *,
    options_input: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_rows = [
        {"id": row.id, "values": deepcopy(row.values)}
        if hasattr(row, "id") and hasattr(row, "values")
        else {"id": row["id"], "values": deepcopy(row["values"])}
        for row in rows_input
    ]
    original_values_by_id = {str(row["id"]): deepcopy(row["values"]) for row in raw_rows}

    df = pd.DataFrame([row["values"] for row in raw_rows])
    df.insert(0, "__id", [str(row["id"]) for row in raw_rows])
    df = df.replace("", pd.NA)

    options = options_input or {}
    drop_duplicate_rows = bool(options.get("drop_duplicate_rows", True))
    duplicate_keys = list(options.get("duplicate_keys") or [])
    keep_columns_not_in_config = bool(options.get("keep_columns_not_in_config", False))
    preserve_original_values = bool(options.get("preserve_original_values", False))
    field_models = [field if isinstance(field, FieldConfig) else FieldConfig.model_validate(field) for field in fields_input]

    removed_duplicates = 0
    if drop_duplicate_rows and not df.empty:
        subset = duplicate_keys if duplicate_keys else None
        before = len(df)
        df = df.drop_duplicates(subset=subset, keep="first").copy()
        removed_duplicates = before - len(df)

    field_reports: list[FieldReport] = []
    total_removed_missing = 0
    total_removed_outliers = 0
    generated_columns: list[str] = []
    generated_column_set: set[str] = set()
    field_configs = {field.key: field for field in field_models}
    pre_normalized_snapshots: dict[str, pd.Series] = {}

    for config in field_models:
        if config.key not in df.columns:
            df[config.key] = pd.NA

        series = df[config.key]
        report = FieldReport(
            key=config.key,
            field_type=config.field_type,
            rows_missing_before=int(_missing_mask(series).sum()),
            rows_missing_after=0,
            rows_removed_as_outliers=0,
            values_clipped=0,
            encoding_applied=config.encoding,
            normalization_applied=config.normalization,
            notes=[],
        )

        if _is_numeric_field(config.field_type) and not df.empty:
            measurement_result = coerce_measurement_series(
                df[config.key],
                field_key=config.key,
                preferred_target_unit=config.target_unit,
            )
            df[config.key] = measurement_result.series
            if measurement_result.note:
                report.notes.append(measurement_result.note)

        missing_mask = _missing_mask(df[config.key])
        removed_missing = 0
        if config.missing_strategy == "drop_row":
            removed_missing = int(missing_mask.sum())
            if removed_missing:
                df = df.loc[~missing_mask].copy()
                report.notes.append("Rows with missing values were removed")
        elif config.missing_strategy != "none":
            fill_value: Any = None
            if config.missing_strategy == "mean":
                numeric = _as_numeric(df[config.key])
                fill_value = float(numeric.mean()) if numeric.notna().any() else None
            elif config.missing_strategy == "median":
                numeric = _as_numeric(df[config.key])
                fill_value = float(numeric.median()) if numeric.notna().any() else None
            elif config.missing_strategy == "mode":
                mode_series = df.loc[~missing_mask, config.key].mode(dropna=True)
                fill_value = mode_series.iloc[0] if not mode_series.empty else None
            elif config.missing_strategy == "constant":
                fill_value = config.missing_constant
            if fill_value is not None:
                df.loc[missing_mask, config.key] = fill_value
                report.notes.append(f"Missing values filled using {config.missing_strategy}")

        removed_outliers = 0
        clipped = 0
        if config.outlier_method != "none" and _is_numeric_field(config.field_type) and not df.empty:
            numeric = _as_numeric(df[config.key])
            valid = numeric.dropna()
            if len(valid) < 2:
                report.notes.append("Outlier handling skipped due to insufficient numeric values")
            else:
                if config.outlier_method.startswith("iqr"):
                    q1 = valid.quantile(0.25)
                    q3 = valid.quantile(0.75)
                    iqr = q3 - q1
                    low = q1 - config.outlier_threshold * iqr
                    high = q3 + config.outlier_threshold * iqr
                else:
                    avg = valid.mean()
                    std = valid.std(ddof=0)
                    if std == 0 or pd.isna(std):
                        low = avg
                        high = avg
                    else:
                        low = avg - config.outlier_threshold * std
                        high = avg + config.outlier_threshold * std

                outlier_mask = numeric.notna() & ((numeric < low) | (numeric > high))
                if config.outlier_method.endswith("remove"):
                    removed_outliers = int(outlier_mask.sum())
                    if removed_outliers:
                        df = df.loc[~outlier_mask].copy()
                        report.notes.append(f"Outliers removed using {config.outlier_method}")
                else:
                    clipped_series = numeric.clip(lower=low, upper=high)
                    clipped = int((numeric.notna() & (numeric != clipped_series)).sum())
                    if clipped:
                        df[config.key] = clipped_series.where(numeric.notna(), df[config.key])
                        report.notes.append(f"Outliers clipped using {config.outlier_method}")

        if config.field_type == "datetime" and not df.empty:
            parsed = df[config.key].map(lambda value: _to_datetime(value, config.datetime_format))
            derived = {
                f"{config.key}__year": parsed.map(lambda value: float(value.year) if value is not None else None),
                f"{config.key}__month": parsed.map(lambda value: float(value.month) if value is not None else None),
                f"{config.key}__day": parsed.map(lambda value: float(value.day) if value is not None else None),
                f"{config.key}__day_of_week": parsed.map(lambda value: float(value.weekday()) if value is not None else None),
            }
            success_count = int(parsed.notna().sum())
            for column, values in derived.items():
                df[column] = values
                if column not in generated_column_set:
                    generated_column_set.add(column)
                    generated_columns.append(column)
            if success_count:
                report.notes.append("Datetime features generated: year, month, day, day_of_week")
            else:
                report.notes.append("Datetime feature generation skipped: no parseable values")

        if config.encoding == "ordinal":
            mapped = df[config.key].map(lambda value: (config.ordinal_map or {}).get(str(value), 0.0) if pd.notna(value) else np.nan)
            df[config.key] = mapped
            report.notes.append("Ordinal encoding applied")
        elif config.encoding == "binary_map":
            mapped = df[config.key].map(
                lambda value: (config.binary_map or {}).get(str(value).lower(), 0.0) if pd.notna(value) else np.nan
            )
            df[config.key] = mapped
            report.notes.append("Binary mapping applied")
        elif config.encoding == "one_hot":
            categories = df[config.key].dropna().astype(str)
            if not categories.empty:
                # Build dummies for non-missing entries, then reindex to full frame and concat once
                dummies = pd.get_dummies(categories, prefix=config.key, prefix_sep="__", dtype=float)
                dummies = dummies.reindex(df.index, fill_value=0.0)
                if not dummies.empty:
                    df = pd.concat([df, dummies], axis=1)
                    for column in dummies.columns:
                        if column not in generated_column_set:
                            generated_column_set.add(column)
                            generated_columns.append(column)
            report.notes.append("One-hot encoding applied")

        if config.field_type == "float" and config.rounding_precision is not None and config.key in df.columns and not df.empty:
            numeric = _as_numeric(df[config.key])
            if numeric.notna().any():
                rounded = numeric.round(config.rounding_precision)
                df[config.key] = rounded.where(numeric.notna(), df[config.key])
                report.notes.append(f"Float values rounded to {config.rounding_precision} decimal places")

        if config.key in df.columns:
            pre_normalized_snapshots[config.key] = df[config.key].copy()

        if config.normalization != "none" and _is_numeric_field(config.field_type) and not df.empty:
            numeric = _as_numeric(df[config.key])
            if numeric.notna().any():
                normalized = _normalize_numeric(numeric, config.normalization).round(6)
                df[config.key] = normalized.where(numeric.notna(), df[config.key])
                report.notes.append(f"Normalization applied: {config.normalization}")
            else:
                report.notes.append("Normalization skipped due to missing numeric values")

        report.rows_missing_after = int(_missing_mask(df[config.key]).sum()) if config.key in df.columns else 0
        report.rows_removed_as_outliers = removed_outliers
        report.values_clipped = clipped
        total_removed_missing += removed_missing
        total_removed_outliers += removed_outliers
        field_reports.append(report)

    if keep_columns_not_in_config:
        output_columns = [column for column in df.columns if column != "__id"]
    else:
        output_columns = [key for key in field_configs if key in df.columns]
        output_columns.extend([column for column in generated_columns if column in df.columns])

    for config in field_models:
        if not config.include_in_output and config.key in output_columns:
            output_columns.remove(config.key)

    selected = df[["__id", *output_columns]].replace({np.nan: None, pd.NA: None})
    pre_normalized_selected = pd.DataFrame({"__id": df["__id"]})
    for column in output_columns:
        source_series = pre_normalized_snapshots.get(column, df[column] if column in df.columns else pd.Series(dtype="object"))
        pre_normalized_selected[column] = source_series
    pre_normalized_selected = pre_normalized_selected.replace({np.nan: None, pd.NA: None})
    records = selected.to_dict(orient="records")
    pre_normalized_records = pre_normalized_selected.to_dict(orient="records")
    pre_normalized_by_id = {
        str(record.get("__id")): {key: value for key, value in record.items() if key != "__id"}
        for record in pre_normalized_records
    }
    output_rows = [
        {
            "id": str(record.pop("__id")),
            "values": record,
            "original_values": deepcopy(original_values_by_id.get(str(record_id)))
            if preserve_original_values
            else None,
            "pre_normalized_values": deepcopy(pre_normalized_by_id.get(str(record_id))) if pre_normalized_by_id.get(str(record_id)) else None,
        }
        for record_id, record in ((row["__id"], row) for row in records)
    ]

    return {
        "summary": {
            "rows_input": len(rows_input),
            "rows_output": len(output_rows),
            "rows_removed_duplicates": removed_duplicates,
            "rows_removed_missing": total_removed_missing,
            "rows_removed_outliers": total_removed_outliers,
            "generated_columns": generated_columns,
        },
        "field_reports": [report.model_dump() for report in field_reports],
        "dataset": output_rows,
    }


def preprocess_dataset(payload: PreprocessingRequest) -> PreprocessingResponse:
    raw_result = preprocess_rows(
        rows_input=payload.dataset.rows,
        fields_input=payload.fields,
        options_input=payload.options.model_dump(),
    )
    return PreprocessingResponse(
        summary=PreprocessingSummary.model_validate(raw_result["summary"]),
        field_reports=[FieldReport.model_validate(item) for item in raw_result["field_reports"]],
        dataset=[ProcessedRow.model_validate(item) for item in raw_result["dataset"]],
    )
