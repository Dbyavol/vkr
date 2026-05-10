from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from app.services.measurement_parsing import coerce_measurement_series
from app.schemas.preprocessing import (
    BoxplotStats,
    ChartPoint,
    DatasetProfileRequest,
    DatasetProfileResponse,
    DatasetQualityIssue,
    DatasetQualityReport,
    FieldConfig,
    FieldProfile,
    FieldRecommendation,
    ProfileDetailLevel,
)

DETAILED_VISUAL_SAMPLE_LIMIT = 5000
DATETIME_FORMAT_CANDIDATES: list[tuple[str, str]] = [
    ("YYYY-MM-DD", "%Y-%m-%d"),
    ("YYYY/MM/DD", "%Y/%m/%d"),
    ("DD.MM.YYYY", "%d.%m.%Y"),
    ("DD-MM-YYYY", "%d-%m-%Y"),
    ("YYYY-MM-DD HH:mm:ss", "%Y-%m-%d %H:%M:%S"),
    ("YYYY/MM/DD HH:mm:ss", "%Y/%m/%d %H:%M:%S"),
    ("DD.MM.YYYY HH:mm:ss", "%d.%m.%Y %H:%M:%S"),
]


def _row_id(row: Any) -> str:
    if isinstance(row, dict):
        return str(row.get("id"))
    return str(row.id)


def _row_values(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        values = row.get("values", {})
        return values if isinstance(values, dict) else {}
    return row.values


def _build_frame(rows: list[Any]) -> pd.DataFrame:
    records = [_row_values(row) for row in rows]
    frame = pd.DataFrame(records, dtype=object)
    frame.insert(0, "__id", [_row_id(row) for row in rows])
    frame = frame.set_index("__id", drop=True)
    frame = frame.replace("", pd.NA)
    return frame


def _non_missing_series(series: pd.Series) -> pd.Series:
    return series.dropna()


def _string_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _numeric_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(float)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    return coerce_measurement_series(series, field_key=series.name or "value").series


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).replace(",", ".").strip()
        if text:
            return float(text)
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


def _detect_datetime_format(values: list[Any]) -> str | None:
    samples = [str(value).strip() for value in values if not _is_missing(value)]
    if not samples:
        return None
    best_label: str | None = None
    best_score = 0
    for label, fmt in DATETIME_FORMAT_CANDIDATES:
        score = 0
        for sample in samples[:50]:
            try:
                datetime.strptime(sample.replace("Z", "+00:00"), fmt)
                score += 1
            except ValueError:
                continue
        if score > best_score:
            best_score = score
            best_label = label
    return best_label if best_score > 0 else None


def _infer_type(series: pd.Series, unique_ratio: float) -> str:
    non_missing = _non_missing_series(series)
    if non_missing.empty:
        return "text"
    lowered = {str(value).strip().lower() for value in non_missing.tolist()}
    if lowered <= {"true", "false", "0", "1", "yes", "no", "да", "нет"} and len(lowered) <= 2:
        return "binary"
    datetime_samples = non_missing.tolist()[: min(50, len(non_missing))]
    datetime_matches = sum(1 for value in datetime_samples if _to_datetime(value) is not None)
    if datetime_samples and float(datetime_matches / len(datetime_samples)) >= 0.8:
        return "datetime"
    numeric = _numeric_series(non_missing)
    numeric_valid = numeric.dropna()
    if len(non_missing) and float(numeric_valid.shape[0] / len(non_missing)) >= 0.9:
        if not numeric_valid.empty and bool((numeric_valid % 1 == 0).all()):
            return "integer"
        return "float"
    if len(lowered) <= 30 and unique_ratio < 0.95:
        return "categorical"
    return "text"


def _histogram(series: pd.Series, bins: int) -> list[ChartPoint]:
    numeric = _numeric_series(series).dropna()
    if numeric.empty:
        return []
    values = numeric.to_numpy(dtype=float)
    min_value = float(values.min())
    max_value = float(values.max())
    if min_value == max_value:
        return [ChartPoint(label=str(round(min_value, 3)), value=int(values.size))]
    counts, edges = np.histogram(values, bins=max(2, bins), range=(min_value, max_value))
    return [
        ChartPoint(
            label=f"{round(float(edges[index]), 2)}-{round(float(edges[index + 1]), 2)}",
            value=int(count),
        )
        for index, count in enumerate(counts.tolist())
    ]


def _category_chart(series: pd.Series, limit: int = 12) -> list[ChartPoint]:
    counts = _string_series(_non_missing_series(series)).value_counts().head(limit)
    return [ChartPoint(label=str(label), value=int(value)) for label, value in counts.items()]


def _ordinal_map(series: pd.Series) -> dict[str, float]:
    unique = sorted({str(value) for value in _non_missing_series(series).tolist()})
    if not unique:
        return {}
    return {value: round((index + 1) / len(unique), 4) for index, value in enumerate(unique)}


def _build_missing_matrix(frame: pd.DataFrame, keys: list[str], limit: int = 120) -> list[dict[str, Any]]:
    if frame.empty or not keys:
        return []
    missing_mask = frame[keys].isna()
    missing_counts = missing_mask.sum(axis=1)
    rows_with_missing = missing_counts[missing_counts > 0].head(limit)
    result: list[dict[str, Any]] = []
    for row_id in rows_with_missing.index.tolist():
        row_mask = missing_mask.loc[row_id]
        missing_fields = row_mask[row_mask].index.tolist()
        result.append(
            {
                "id": str(row_id),
                "missing_count": int(rows_with_missing.loc[row_id]),
                "missing_fields": missing_fields,
            }
        )
    return result


def _build_missing_rows_preview(frame: pd.DataFrame, keys: list[str], limit: int = 14) -> list[dict[str, Any]]:
    if frame.empty or not keys:
        return []
    missing_mask = frame[keys].isna()
    missing_counts = missing_mask.sum(axis=1)
    rows_with_missing = missing_counts[missing_counts > 0].head(limit)
    result: list[dict[str, Any]] = []
    for row_id in rows_with_missing.index.tolist():
        row_values = {
            key: (None if pd.isna(value) else value)
            for key, value in frame.loc[row_id].to_dict().items()
        }
        result.append({"id": str(row_id), "values": row_values})
    return result


def _build_correlation_matrix(frame: pd.DataFrame, numeric_keys: list[str], limit: int = 60) -> list[dict[str, Any]]:
    if len(numeric_keys) < 2:
        return []
    numeric_frame = pd.DataFrame({key: _numeric_series(frame[key]) for key in numeric_keys}, index=frame.index)
    corr = numeric_frame.corr(method="pearson", min_periods=3)
    pairs: list[dict[str, Any]] = []
    for index, left_key in enumerate(numeric_keys):
        for right_key in numeric_keys[index + 1 :]:
            pearson = corr.at[left_key, right_key]
            if pd.isna(pearson):
                continue
            samples = int(numeric_frame[[left_key, right_key]].dropna().shape[0])
            pairs.append(
                {
                    "left_key": left_key,
                    "right_key": right_key,
                    "pearson": round(float(pearson), 4),
                    "samples": samples,
                }
            )
    pairs.sort(key=lambda item: abs(float(item["pearson"])), reverse=True)
    return pairs[:limit]


def _profile_field(
    key: str,
    series: pd.Series,
    visual_series: pd.Series | None,
    rows_total: int,
    max_unique_values: int,
    histogram_bins: int,
    *,
    include_visuals: bool,
) -> FieldProfile:
    values = series.tolist()
    non_missing_series = _non_missing_series(series)
    non_missing = non_missing_series.tolist()
    unique_count = int(_string_series(non_missing_series).nunique()) if not non_missing_series.empty else 0
    unique_ratio = round(unique_count / max(len(non_missing), 1), 4)
    inferred_type = _infer_type(series, unique_ratio)
    numeric_series = _numeric_series(non_missing_series).dropna()
    numeric_values = numeric_series.tolist()
    missing_count = int(series.isna().sum())

    recommendations: list[FieldRecommendation] = []
    config = FieldConfig(
        key=key,
        field_type=inferred_type,  # type: ignore[arg-type]
        include_in_output=inferred_type != "text",
    )
    measurement_result = coerce_measurement_series(series, field_key=key)
    if measurement_result.detected_unit_family:
        config.unit_family = measurement_result.detected_unit_family
    if measurement_result.target_unit:
        config.target_unit = measurement_result.target_unit

    outlier_count = 0
    numeric_min = numeric_max = numeric_mean = numeric_median = None
    histogram: list[ChartPoint] = []
    boxplot_stats: BoxplotStats | None = None
    top_categories = _category_chart(visual_series if visual_series is not None else series) if include_visuals else []

    if inferred_type in {"numeric", "integer", "float"} and numeric_values:
        q1 = float(numeric_series.quantile(0.25))
        q3 = float(numeric_series.quantile(0.75))
        iqr = q3 - q1
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        outlier_count = int(((numeric_series < low) | (numeric_series > high)).sum())
        numeric_min = float(numeric_series.min())
        numeric_max = float(numeric_series.max())
        numeric_mean = float(numeric_series.mean())
        numeric_median = float(numeric_series.median())
        boxplot_stats = BoxplotStats(
            min=numeric_min,
            q1=q1,
            median=numeric_median,
            q3=q3,
            max=numeric_max,
        )
        if include_visuals:
            histogram = _histogram(numeric_series, histogram_bins)
        recommendations.append(
            FieldRecommendation(
                code="NORMALIZE_NUMERIC",
                severity="info",
                message="Для метода взвешенных коэффициентов числовой признак лучше нормализовать.",
                suggested_patch={"normalization": "minmax"},
            )
        )
        if missing_count:
            recommendations.append(
                FieldRecommendation(
                    code="FILL_NUMERIC_MISSING",
                    severity="warning",
                    message="Есть пропуски. Для числового признака обычно безопасна медиана.",
                    suggested_patch={"missing_strategy": "median"},
                )
            )
        if outlier_count:
            recommendations.append(
                FieldRecommendation(
                    code="HANDLE_OUTLIERS",
                    severity="warning",
                    message=f"Найдено возможных выбросов по IQR: {outlier_count}.",
                    suggested_patch={"outlier_method": "iqr_clip"},
                )
            )
        if inferred_type == "float":
            config.rounding_precision = 2
            recommendations.append(
                FieldRecommendation(
                    code="ROUND_FLOAT_VALUES",
                    severity="info",
                    message="Для дробного признака можно задать округление на этапе подготовки данных.",
                    suggested_patch={"rounding_precision": 2},
                )
            )
        if measurement_result.note:
            recommendations.append(
                FieldRecommendation(
                    code="NORMALIZE_UNITS",
                    severity="info",
                    message=measurement_result.note,
                    suggested_patch={
                        "field_type": inferred_type,
                        "unit_family": measurement_result.detected_unit_family,
                        "target_unit": measurement_result.target_unit,
                    },
                )
            )

    if inferred_type == "categorical":
        config.encoding = "ordinal"
        config.ordinal_map = _ordinal_map(series)
        recommendations.append(
            FieldRecommendation(
                code="ENCODE_CATEGORICAL",
                severity="info",
                message="Категориальный признак можно включить в расчет через ordinal encoding, если категории имеют порядок предпочтения.",
                suggested_patch={"encoding": "ordinal", "ordinal_map": config.ordinal_map},
            )
        )
        if missing_count:
            recommendations.append(
                FieldRecommendation(
                    code="FILL_CATEGORICAL_MISSING",
                    severity="warning",
                    message="Есть пропуски. Для категорий можно использовать наиболее частое значение.",
                    suggested_patch={"missing_strategy": "mode"},
                )
            )

    if inferred_type == "binary":
        config.binary_map = {"true": 1, "false": 0, "1": 1, "0": 0, "yes": 1, "no": 0, "да": 1, "нет": 0}
        recommendations.append(
            FieldRecommendation(
                code="ENCODE_BINARY",
                severity="info",
                message="Бинарный признак можно напрямую преобразовать в шкалу 0/1.",
                suggested_patch={"encoding": "binary_map", "binary_map": config.binary_map},
            )
        )

    if inferred_type == "datetime":
        detected_datetime_format = _detect_datetime_format(non_missing)
        config.include_in_output = False
        config.datetime_format = detected_datetime_format
        recommendations.append(
            FieldRecommendation(
                code="DERIVE_DATETIME_FEATURES",
                severity="info",
                message="Datetime-признак лучше разложить на производные поля: год, месяц, день и день недели.",
                suggested_patch={"field_type": "datetime", "include_in_output": False, "datetime_format": detected_datetime_format},
            )
        )
        if missing_count:
            recommendations.append(
                FieldRecommendation(
                    code="FILL_DATETIME_MISSING",
                    severity="warning",
                    message="Есть пропуски в datetime-поле, проверьте заполнение или удаление строк перед генерацией признаков.",
                    suggested_patch={"missing_strategy": "none"},
                )
            )

    text_to_categorical_possible = inferred_type == "text" and 1 < unique_count <= max_unique_values and unique_ratio <= 0.7
    if text_to_categorical_possible:
        config.field_type = "categorical"
        config.include_in_output = False
        config.encoding = "ordinal"
        config.ordinal_map = _ordinal_map(series)
        recommendations.append(
            FieldRecommendation(
                code="TEXT_CAN_BE_CATEGORY",
                severity="info",
                message="Текстовый признак имеет ограниченное число уникальных значений, его можно рассмотреть как категориальный.",
                suggested_patch={"field_type": "categorical", "encoding": "ordinal", "ordinal_map": config.ordinal_map},
            )
        )
    elif inferred_type == "text":
        recommendations.append(
            FieldRecommendation(
                code="EXCLUDE_TEXT",
                severity="info",
                message="Свободный текст лучше исключить из расчета взвешенных коэффициентов или обрабатывать отдельным NLP-модулем.",
                suggested_patch={"include_in_output": False},
            )
        )

    return FieldProfile(
        key=key,
        inferred_type=config.field_type,
        analytic_candidate=config.include_in_output,
        detected_unit_family=measurement_result.detected_unit_family,
        detected_units=measurement_result.detected_units or [],
        target_unit=measurement_result.target_unit,
        rows_total=rows_total,
        missing_count=missing_count,
        unique_count=unique_count,
        unique_ratio=unique_ratio,
        sample_values=non_missing[:8],
        numeric_min=numeric_min,
        numeric_max=numeric_max,
        numeric_mean=round(numeric_mean, 4) if numeric_mean is not None else None,
        numeric_median=numeric_median,
        outlier_count_iqr=outlier_count,
        histogram=histogram,
        boxplot_stats=boxplot_stats,
        top_categories=top_categories,
        text_to_categorical_possible=text_to_categorical_possible,
        recommended_config=config,
        recommendations=recommendations,
    )


def _sample_rows_for_detailed_visuals(rows: list[Any], limit: int = DETAILED_VISUAL_SAMPLE_LIMIT) -> list[Any]:
    if len(rows) <= limit:
        return rows
    step = max(1, len(rows) // limit)
    sampled = rows[::step]
    return sampled[:limit]


def _sample_values(values: list[Any], source_rows_total: int, limit: int = DETAILED_VISUAL_SAMPLE_LIMIT) -> list[Any]:
    if len(values) <= limit or source_rows_total <= limit:
        return values
    step = max(1, len(values) // limit)
    sampled = values[::step]
    return sampled[:limit]


def profile_rows(
    rows: list[Any],
    *,
    max_unique_values: int = 30,
    histogram_bins: int = 8,
    histogram_bins_by_field: dict[str, int] | None = None,
    detail_level: ProfileDetailLevel = "detailed",
) -> DatasetProfileResponse:
    include_visuals = detail_level == "detailed"
    frame = _build_frame(rows)
    rows_for_visuals = _sample_rows_for_detailed_visuals(rows) if include_visuals else rows
    visual_frame = _build_frame(rows_for_visuals) if include_visuals else frame
    keys = sorted(frame.columns.tolist())
    bins_by_field = {
        key: max(2, min(64, int(value)))
        for key, value in (histogram_bins_by_field or {}).items()
    }
    fields = [
        _profile_field(
            key,
            frame[key] if key in frame.columns else pd.Series(dtype="object"),
            pd.Series(_sample_values(visual_frame[key].tolist(), len(frame.index))) if include_visuals and key in visual_frame.columns else None,
            len(frame.index),
            max_unique_values,
            bins_by_field.get(key, histogram_bins),
            include_visuals=include_visuals,
        )
        for key in keys
    ]
    weights, notes = _recommended_weights(fields)
    numeric_keys = [field.key for field in fields if field.inferred_type in {"numeric", "integer", "float"}]
    return DatasetProfileResponse(
        rows_total=len(frame.index),
        detail_level=detail_level,
        fields=fields,
        quality=_quality_report(fields, len(frame.index)),
        recommended_weights=weights,
        weight_notes=notes,
        missing_matrix_preview=_build_missing_matrix(visual_frame, keys) if include_visuals else [],
        missing_rows_preview=_build_missing_rows_preview(frame, keys) if include_visuals else [],
        correlation_matrix=_build_correlation_matrix(visual_frame, numeric_keys) if include_visuals else [],
    )


def profile_dataset(payload: DatasetProfileRequest) -> DatasetProfileResponse:
    return profile_rows(
        payload.dataset.rows,
        max_unique_values=payload.max_unique_values,
        histogram_bins=payload.histogram_bins,
        histogram_bins_by_field=payload.histogram_bins_by_field or {},
        detail_level=payload.detail_level,
    )


def _quality_report(fields: list[FieldProfile], rows_total: int) -> DatasetQualityReport:
    issues: list[DatasetQualityIssue] = []
    fields_count = max(len(fields), 1)
    analytic_fields = [field for field in fields if field.analytic_candidate]
    numeric_fields = [field for field in fields if field.inferred_type in {"numeric", "integer", "float"}]
    categorical_fields = [field for field in fields if field.inferred_type == "categorical"]
    text_fields = [field for field in fields if field.inferred_type == "text"]
    total_missing = sum(field.missing_count for field in fields)
    total_outliers = sum(field.outlier_count_iqr for field in fields)
    total_cells = max(rows_total * fields_count, 1)
    missing_ratio = total_missing / total_cells

    score = 100.0
    if not analytic_fields:
        issues.append(
            DatasetQualityIssue(
                code="NO_ANALYTIC_FIELDS",
                severity="critical",
                message="No fields are currently suitable for comparative analysis.",
                penalty=45,
            )
        )
        score -= 45
    elif len(analytic_fields) < 3:
        issues.append(
            DatasetQualityIssue(
                code="FEW_ANALYTIC_FIELDS",
                severity="warning",
                message="Dataset has fewer than 3 analytic fields, comparison may be unstable.",
                affected_fields=[field.key for field in analytic_fields],
                penalty=12,
            )
        )
        score -= 12

    if missing_ratio > 0:
        penalty = min(30.0, missing_ratio * 120)
        missing_fields = [field.key for field in fields if field.missing_count]
        issues.append(
            DatasetQualityIssue(
                code="MISSING_VALUES",
                severity="critical" if missing_ratio >= 0.25 else "warning",
                message=f"Dataset contains missing values in {len(missing_fields)} fields.",
                affected_fields=missing_fields,
                penalty=round(penalty, 2),
            )
        )
        score -= penalty

    outlier_fields = [field.key for field in numeric_fields if field.outlier_count_iqr]
    if outlier_fields:
        outlier_ratio = total_outliers / max(rows_total * max(len(numeric_fields), 1), 1)
        penalty = min(18.0, 6 + outlier_ratio * 80)
        issues.append(
            DatasetQualityIssue(
                code="OUTLIERS",
                severity="warning",
                message=f"IQR-based profiling found possible outliers in {len(outlier_fields)} numeric fields.",
                affected_fields=outlier_fields,
                penalty=round(penalty, 2),
            )
        )
        score -= penalty

    high_unique_fields = [
        field.key
        for field in fields
        if field.inferred_type != "numeric" and field.unique_ratio > 0.9 and field.unique_count > 10
    ]
    if high_unique_fields:
        penalty = min(14.0, 4 + 2 * len(high_unique_fields))
        issues.append(
            DatasetQualityIssue(
                code="HIGH_CARDINALITY",
                severity="warning",
                message="Some non-numeric fields look like identifiers or free text and are weak criteria.",
                affected_fields=high_unique_fields,
                penalty=round(penalty, 2),
            )
        )
        score -= penalty

    constant_fields = [
        field.key
        for field in fields
        if field.unique_count <= 1 and field.rows_total > 1
    ]
    if constant_fields:
        penalty = min(10.0, 2 * len(constant_fields))
        issues.append(
            DatasetQualityIssue(
                code="CONSTANT_FIELDS",
                severity="info",
                message="Some fields have only one unique value and will not influence ranking.",
                affected_fields=constant_fields,
                penalty=round(penalty, 2),
            )
        )
        score -= penalty

    if rows_total < 5:
        issues.append(
            DatasetQualityIssue(
                code="SMALL_DATASET",
                severity="warning",
                message="Dataset is very small; analog search and statistics may be fragile.",
                penalty=8,
            )
        )
        score -= 8

    score = round(max(0.0, min(100.0, score)), 2)
    if score >= 85:
        level = "good"
        label = "Готово к анализу, можно использовать без изменений или с легкой предобработкой"
    elif score >= 65:
        level = "medium"
        label = "Можно использовать, но нужно проверить рекомендации"
    elif score >= 40:
        level = "risky"
        label = "Рискованно, рекомендуется сделать предобработку и проверить рекомендации"
    else:
        level = "poor"
        label = "Не готово к анализу, рекомендуется серьезная предобработка и проверка рекомендаций"

    return DatasetQualityReport(
        score=score,
        level=level,
        readiness_label=label,
        analytic_fields_count=len(analytic_fields),
        numeric_fields_count=len(numeric_fields),
        categorical_fields_count=len(categorical_fields),
        text_fields_count=len(text_fields),
        total_missing_values=total_missing,
        total_outliers_iqr=total_outliers,
        issues=issues,
    )


def _recommended_weights(fields: list[FieldProfile]) -> tuple[dict[str, float], list[str]]:
    scores: dict[str, float] = {}
    notes: list[str] = []
    for field in fields:
        if not field.analytic_candidate:
            continue
        score = 1.0
        if field.inferred_type in {"numeric", "integer", "float"}:
            score += 0.35
            if field.numeric_min is not None and field.numeric_max is not None and field.numeric_min != field.numeric_max:
                score += 0.25
            if field.outlier_count_iqr:
                score -= 0.15
        elif field.inferred_type == "binary":
            score += 0.1
        elif field.inferred_type == "categorical":
            score += 0.05
            if field.unique_count > 12:
                score -= 0.25
        if field.missing_count:
            missing_ratio = field.missing_count / max(field.rows_total, 1)
            score -= min(0.45, missing_ratio)
        if field.unique_ratio > 0.9 and field.inferred_type != "numeric":
            score -= 0.35
        scores[field.key] = max(score, 0.05)

    total = sum(scores.values())
    if total <= 0:
        return {}, ["Нет подходящих аналитических признаков для автоматической рекомендации весов."]

    weights = {key: round(value / total, 4) for key, value in scores.items()}
    drift = round(1.0 - sum(weights.values()), 4)
    if weights and drift:
        first_key = next(iter(weights))
        weights[first_key] = round(weights[first_key] + drift, 4)
    notes.append("Веса рассчитаны эвристически: выше вес у числовых информативных признаков, ниже у признаков с пропусками, выбросами и высокой уникальностью.")
    notes.append("Рекомендация является стартовой точкой. Пользователь должен подтвердить веса с учетом предметной области.")
    return weights, notes
