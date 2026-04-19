from __future__ import annotations

from collections import Counter
from statistics import mean, median
from typing import Any

from app.schemas.preprocessing import (
    ChartPoint,
    DatasetProfileRequest,
    DatasetProfileResponse,
    DatasetQualityIssue,
    DatasetQualityReport,
    FieldConfig,
    FieldProfile,
    FieldRecommendation,
)


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


def _percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    index = (len(sorted_values) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    if lower == upper:
        return sorted_values[lower]
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (index - lower)


def _infer_type(values: list[Any], unique_ratio: float) -> str:
    non_missing = [value for value in values if not _is_missing(value)]
    if not non_missing:
        return "text"
    lowered = {str(value).strip().lower() for value in non_missing}
    if lowered <= {"true", "false", "0", "1", "yes", "no", "да", "нет"} and len(lowered) <= 2:
        return "binary"
    numeric_count = sum(1 for value in non_missing if _to_float(value) is not None)
    if numeric_count / len(non_missing) >= 0.9:
        return "numeric"
    if len(lowered) <= 30 and unique_ratio < 0.95:
        return "categorical"
    return "text"


def _histogram(values: list[float], bins: int) -> list[ChartPoint]:
    if not values:
        return []
    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        return [ChartPoint(label=str(round(min_value, 3)), value=len(values))]
    bins = max(2, bins)
    width = (max_value - min_value) / bins
    counts = [0 for _ in range(bins)]
    for value in values:
        index = min(int((value - min_value) / width), bins - 1)
        counts[index] += 1
    return [
        ChartPoint(label=f"{round(min_value + width * index, 2)}-{round(min_value + width * (index + 1), 2)}", value=count)
        for index, count in enumerate(counts)
    ]


def _category_chart(values: list[Any], limit: int = 12) -> list[ChartPoint]:
    counter = Counter(str(value) for value in values if not _is_missing(value))
    return [ChartPoint(label=label, value=count) for label, count in counter.most_common(limit)]


def _ordinal_map(values: list[Any]) -> dict[str, float]:
    unique = sorted({str(value) for value in values if not _is_missing(value)})
    if not unique:
        return {}
    return {value: round((index + 1) / len(unique), 4) for index, value in enumerate(unique)}


def _profile_field(key: str, values: list[Any], rows_total: int, max_unique_values: int, histogram_bins: int) -> FieldProfile:
    missing_count = sum(1 for value in values if _is_missing(value))
    non_missing = [value for value in values if not _is_missing(value)]
    unique_values = sorted({str(value) for value in non_missing})
    unique_count = len(unique_values)
    unique_ratio = round(unique_count / max(len(non_missing), 1), 4)
    inferred_type = _infer_type(values, unique_ratio)
    numeric_values = [numeric for value in non_missing if (numeric := _to_float(value)) is not None]

    recommendations: list[FieldRecommendation] = []
    config = FieldConfig(
        key=key,
        field_type=inferred_type,  # type: ignore[arg-type]
        include_in_output=inferred_type != "text",
    )

    outlier_count = 0
    numeric_min = numeric_max = numeric_mean = numeric_median = None
    histogram: list[ChartPoint] = []
    top_categories = _category_chart(values)

    if inferred_type == "numeric" and numeric_values:
        ordered = sorted(numeric_values)
        q1 = _percentile(ordered, 0.25)
        q3 = _percentile(ordered, 0.75)
        iqr = q3 - q1
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        outlier_count = sum(1 for value in numeric_values if value < low or value > high)
        numeric_min = min(numeric_values)
        numeric_max = max(numeric_values)
        numeric_mean = mean(numeric_values)
        numeric_median = median(numeric_values)
        histogram = _histogram(numeric_values, histogram_bins)
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

    if inferred_type == "categorical":
        config.ordinal_map = _ordinal_map(values)
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

    text_to_categorical_possible = inferred_type == "text" and 1 < unique_count <= max_unique_values and unique_ratio <= 0.7
    if text_to_categorical_possible:
        config.field_type = "categorical"
        config.include_in_output = False
        config.ordinal_map = _ordinal_map(values)
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
                message="Свободный текст лучше исключить из расчета взвешенных коэффициентов или обработать отдельным NLP-модулем.",
                suggested_patch={"include_in_output": False},
            )
        )

    return FieldProfile(
        key=key,
        inferred_type=config.field_type,
        analytic_candidate=config.include_in_output,
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
        top_categories=top_categories,
        text_to_categorical_possible=text_to_categorical_possible,
        recommended_config=config,
        recommendations=recommendations,
    )


def profile_dataset(payload: DatasetProfileRequest) -> DatasetProfileResponse:
    rows = payload.dataset.rows
    keys = sorted({key for row in rows for key in row.values.keys()})
    fields = [
        _profile_field(
            key,
            [row.values.get(key) for row in rows],
            len(rows),
            payload.max_unique_values,
            payload.histogram_bins,
        )
        for key in keys
    ]
    weights, notes = _recommended_weights(fields)
    return DatasetProfileResponse(
        rows_total=len(rows),
        fields=fields,
        quality=_quality_report(fields, len(rows)),
        recommended_weights=weights,
        weight_notes=notes,
    )


def _quality_report(fields: list[FieldProfile], rows_total: int) -> DatasetQualityReport:
    issues: list[DatasetQualityIssue] = []
    fields_count = max(len(fields), 1)
    analytic_fields = [field for field in fields if field.analytic_candidate]
    numeric_fields = [field for field in fields if field.inferred_type == "numeric"]
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
        label = "Ready for analysis"
    elif score >= 65:
        level = "medium"
        label = "Usable, but review recommendations"
    elif score >= 40:
        level = "risky"
        label = "Risky, preprocessing is recommended"
    else:
        level = "poor"
        label = "Not ready for reliable analysis"

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
        if field.inferred_type == "numeric":
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
