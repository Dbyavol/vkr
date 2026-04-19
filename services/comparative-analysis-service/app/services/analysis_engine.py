from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.schemas.analysis import (
    AnalogGroup,
    AnalysisRequest,
    AnalysisResponse,
    AnalysisSummary,
    CriterionConfig,
    CriterionContribution,
    CriterionSensitivity,
    DominancePair,
    RankedObject,
)


@dataclass
class NormalizedCriterionValue:
    value: float
    note: str | None = None


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip().replace(" ", "").replace(",", ".")
        if not normalized:
            return None
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _normalize_numeric(raw_value: Any, criterion: CriterionConfig, values: list[float]) -> NormalizedCriterionValue:
    numeric = _to_float(raw_value)
    if numeric is None:
        return NormalizedCriterionValue(0.0, "Значение отсутствует или не является числом")
    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        return NormalizedCriterionValue(1.0, "У всех объектов одинаковое значение по этому критерию")
    if criterion.direction == "minimize":
        return NormalizedCriterionValue((max_value - numeric) / (max_value - min_value))
    return NormalizedCriterionValue((numeric - min_value) / (max_value - min_value))


def _normalize_categorical(raw_value: Any, criterion: CriterionConfig) -> NormalizedCriterionValue:
    if criterion.direction == "target":
        return NormalizedCriterionValue(1.0 if raw_value == criterion.target_value else 0.0)
    if criterion.scale_map:
        return NormalizedCriterionValue(criterion.scale_map.get(str(raw_value), 0.0))
    if isinstance(raw_value, bool):
        return NormalizedCriterionValue(1.0 if raw_value else 0.0)
    return NormalizedCriterionValue(0.0, "Для категориального критерия не задана шкала")


def _normalize_value(raw_value: Any, criterion: CriterionConfig, dataset_values: list[Any]) -> NormalizedCriterionValue:
    if criterion.type == "numeric":
        numeric_values = [value for item in dataset_values if (value := _to_float(item)) is not None]
        if not numeric_values:
            return NormalizedCriterionValue(0.0, "В датасете нет числовых значений по этому критерию")
        return _normalize_numeric(raw_value, criterion, numeric_values)
    return _normalize_categorical(raw_value, criterion)


def _weights(criteria: list[CriterionConfig], auto_normalize: bool) -> tuple[dict[str, float], list[str]]:
    raw_sum = sum(item.weight for item in criteria)
    if raw_sum <= 0:
        raise ValueError("The sum of weights must be greater than zero")
    notes: list[str] = []
    if abs(raw_sum - 1.0) > 1e-9:
        if not auto_normalize:
            raise ValueError("The sum of weights must be 1.0 when auto_normalize_weights=false")
        notes.append(f"Веса были автоматически нормализованы: {raw_sum:.4f} -> 1.0000")
    return {item.key: item.weight / raw_sum for item in criteria}, notes


def _target_similarity(contributions: list[CriterionContribution], target_contributions: dict[str, float] | None) -> float | None:
    if not target_contributions:
        return None
    deltas = [
        abs(item.normalized_value - target_contributions.get(item.key, 0.0))
        for item in contributions
    ]
    if not deltas:
        return None
    return round(1.0 - (sum(deltas) / len(deltas)), 4)


def _weighted_target_similarity(contributions: list[CriterionContribution], target_contributions: dict[str, float] | None) -> float | None:
    if not target_contributions:
        return None
    similarity = 0.0
    total_weight = 0.0
    for item in contributions:
        target_value = target_contributions.get(item.key)
        if target_value is None:
            continue
        similarity += max(0.0, 1.0 - abs(item.normalized_value - target_value)) * item.weight
        total_weight += item.weight
    if total_weight <= 0:
        return None
    return round(similarity / total_weight, 4)


def _criterion_sensitivity(rows: list[RankedObject], criteria: list[CriterionConfig]) -> list[CriterionSensitivity]:
    result: list[CriterionSensitivity] = []
    by_key = {criterion.key: criterion for criterion in criteria}
    for key, criterion in by_key.items():
        values = [
            contribution.normalized_value
            for row in rows
            for contribution in row.contributions
            if contribution.key == key
        ]
        if not values:
            continue
        normalized_range = max(values) - min(values)
        weight = next((item.weight for row in rows for item in row.contributions if item.key == key), 0.0)
        sensitivity_index = round(normalized_range * weight, 4)
        if normalized_range == 0:
            note = "Критерий не имеет разброса и не влияет на ранжирование."
        elif sensitivity_index >= 0.2:
            note = "Рейтинг сильно чувствителен к этому критерию."
        elif sensitivity_index >= 0.08:
            note = "Критерий умеренно влияет на ранжирование."
        else:
            note = "При текущих весах критерий влияет слабо."
        result.append(
            CriterionSensitivity(
                key=key,
                name=criterion.name,
                weight=round(weight, 4),
                normalized_range=round(normalized_range, 4),
                sensitivity_index=sensitivity_index,
                note=note,
            )
        )
    return sorted(result, key=lambda item: item.sensitivity_index, reverse=True)


def _ranking_stability_note(rows: list[RankedObject]) -> str:
    if len(rows) < 2:
        return "Доступен только один объект сравнения, устойчивость рейтинга оценить нельзя."
    margin = rows[0].score - rows[1].score
    if margin >= 0.15:
        return f"Рейтинг устойчив: лидер опережает второй объект на {margin:.4f}."
    if margin >= 0.05:
        return f"Рейтинг умеренно устойчив: отрыв лидера составляет {margin:.4f}."
    return f"Рейтинг чувствителен: отрыв лидера составляет только {margin:.4f}."


def _analog_groups(rows: list[RankedObject]) -> list[AnalogGroup]:
    groups = [
        ("Очень близкие аналоги", 0.85, 1.0),
        ("Умеренно близкие аналоги", 0.65, 0.8499),
        ("Слабые аналоги", 0.0, 0.6499),
    ]
    result: list[AnalogGroup] = []
    for label, low, high in groups:
        ids = [row.object_id for row in rows if low <= row.score <= high]
        if ids:
            result.append(AnalogGroup(label=label, min_score=low, max_score=high, object_ids=ids))
    return result


def _dominance_pairs(rows: list[RankedObject], limit: int = 8) -> list[DominancePair]:
    pairs: list[DominancePair] = []
    normalized_by_object = {
        row.object_id: {item.key: item.normalized_value for item in row.contributions}
        for row in rows
    }
    object_ids = list(normalized_by_object)
    for left_id in object_ids:
        for right_id in object_ids:
            if left_id == right_id:
                continue
            left = normalized_by_object[left_id]
            right = normalized_by_object[right_id]
            keys = sorted(set(left) & set(right))
            if not keys:
                continue
            at_least_equal = all(left[key] >= right[key] for key in keys)
            strictly_better = any(left[key] > right[key] for key in keys)
            if at_least_equal and strictly_better:
                pairs.append(
                    DominancePair(
                        dominant_object_id=left_id,
                        dominated_object_id=right_id,
                        criteria_count=len(keys),
                    )
                )
            if len(pairs) >= limit:
                return pairs
    return pairs


def _confidence(rows: list[RankedObject], criteria_count: int) -> tuple[float, list[str]]:
    notes: list[str] = []
    missing_notes = sum(
        1
        for row in rows
        for contribution in row.contributions
        if contribution.note
        and (
            "отсутствует" in contribution.note
            or "не является числом" in contribution.note
            or "нет числовых" in contribution.note
            or "не задана шкала" in contribution.note
        )
    )
    total_cells = max(len(rows) * criteria_count, 1)
    missing_ratio = missing_notes / total_cells
    confidence = 1.0 - min(0.55, missing_ratio)
    flat_criteria = {
        contribution.key
        for row in rows
        for contribution in row.contributions
        if contribution.note and "одинаковое значение" in contribution.note
    }
    if missing_ratio:
        notes.append(f"{missing_ratio:.1%} значений критериев отсутствуют или недоступны для расчета.")
    if flat_criteria:
        confidence -= min(0.25, 0.05 * len(flat_criteria))
        notes.append(f"{len(flat_criteria)} критериев имеют одинаковые значения и не влияют на ранжирование.")
    if criteria_count < 3:
        confidence -= 0.1
        notes.append("Используется мало активных критериев, результат следует интерпретировать осторожно.")
    if not notes:
        notes.append("Датасет достаточно полный для выбранных критериев.")
    return round(max(0.0, min(1.0, confidence)), 4), notes


def run_comparative_analysis(payload: AnalysisRequest) -> AnalysisResponse:
    criteria_by_key = {item.key: item for item in payload.criteria}
    normalized_weights, notes = _weights(payload.criteria, payload.auto_normalize_weights)

    dataset_values = {
        criterion.key: [obj.attributes.get(criterion.key) for obj in payload.dataset.objects]
        for criterion in payload.criteria
    }

    rows: list[RankedObject] = []
    target_contributions: dict[str, float] | None = None

    for obj in payload.dataset.objects:
        contributions: list[CriterionContribution] = []
        score = 0.0
        for key, criterion in criteria_by_key.items():
            raw_value = obj.attributes.get(key)
            normalized = _normalize_value(raw_value, criterion, dataset_values[key])
            weight = normalized_weights[key]
            contribution = normalized.value * weight
            score += contribution
            contributions.append(
                CriterionContribution(
                    key=key,
                    name=criterion.name,
                    raw_value=raw_value,
                    normalized_value=round(normalized.value, 4),
                    weight=round(weight, 4),
                    contribution=round(contribution, 4),
                    note=normalized.note,
                )
            )
        if payload.target_object_id and obj.id == payload.target_object_id:
            target_contributions = {item.key: item.normalized_value for item in contributions}
        top_positive = sorted(contributions, key=lambda item: item.contribution, reverse=True)[:3]
        top_negative = sorted(contributions, key=lambda item: item.contribution)[:2]
        explanation = (
            f"Сильнее всего оценку повысили: {', '.join(item.name for item in top_positive) or 'нет данных'}. "
            f"Слабее всего повлияли: {', '.join(item.name for item in top_negative) or 'нет данных'}."
        )
        rows.append(
            RankedObject(
                object_id=obj.id,
                title=obj.title,
                rank=0,
                score=round(score, 4),
                contributions=contributions,
                similarity_to_target=None,
                explanation=explanation,
            )
        )

    for row in rows:
        row.similarity_to_target = _weighted_target_similarity(row.contributions, target_contributions)
        if payload.mode == "analog_search" and row.similarity_to_target is not None:
            row.score = row.similarity_to_target
            row.explanation = (
                f"Сходство с целевым объектом: {row.similarity_to_target:.4f}. "
                f"Расчет выполнен по взвешенной близости нормализованных критериев."
            )

    if payload.mode == "analog_search" and payload.target_object_id:
        rows = [row for row in rows if row.object_id != payload.target_object_id]

    rows.sort(
        key=lambda item: (
            item.similarity_to_target if item.similarity_to_target is not None else -1,
            item.score,
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row.rank = index

    best = rows[0]
    confidence_score, confidence_notes = _confidence(rows, len(payload.criteria))
    return AnalysisResponse(
        summary=AnalysisSummary(
            objects_count=len(payload.dataset.objects),
            criteria_count=len(payload.criteria),
            weights_sum=round(sum(normalized_weights.values()), 4),
            best_object_id=best.object_id,
            best_score=best.score,
            normalization_notes=notes,
            mode=payload.mode,
            target_object_id=payload.target_object_id,
            confidence_score=confidence_score,
            confidence_notes=confidence_notes,
            sensitivity=_criterion_sensitivity(rows, payload.criteria),
            ranking_stability_note=_ranking_stability_note(rows),
            analog_groups=_analog_groups(rows) if payload.mode == "analog_search" else [],
            dominance_pairs=_dominance_pairs(rows),
        ),
        ranking=rows[: payload.top_n],
    )
