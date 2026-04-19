from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.schemas.analysis import (
    AnalogGroup,
    AnalysisFilters,
    AnalysisRequest,
    AnalysisResponse,
    AnalysisSummary,
    CategoricalAllowlistFilter,
    CriterionConfig,
    CriterionContribution,
    CriterionSensitivity,
    DominancePair,
    NumericRangeFilter,
    RankedObject,
    RankingStabilityScenario,
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


def _passes_numeric_filter(attributes: dict[str, Any], filter_config: NumericRangeFilter) -> bool:
    numeric_value = _to_float(attributes.get(filter_config.key))
    if numeric_value is None:
        return False
    if filter_config.min_value is not None and numeric_value < filter_config.min_value:
        return False
    if filter_config.max_value is not None and numeric_value > filter_config.max_value:
        return False
    return True


def _passes_categorical_filter(attributes: dict[str, Any], filter_config: CategoricalAllowlistFilter) -> bool:
    value = attributes.get(filter_config.key)
    if value is None:
        return False
    allowed = {item for item in filter_config.values}
    return str(value) in allowed


def _apply_filters(payload: AnalysisRequest) -> list[Any]:
    filters: AnalysisFilters | None = payload.filter_criteria
    if not filters:
        return list(payload.dataset.objects)

    filtered = []
    for obj in payload.dataset.objects:
        if not all(_passes_numeric_filter(obj.attributes, item) for item in filters.numeric_ranges):
            continue
        if not all(_passes_categorical_filter(obj.attributes, item) for item in filters.categorical_allowlist):
            continue
        filtered.append(obj)

    # Keep target in analog mode as a reference point for similarity, even if filters exclude it.
    if payload.mode == "analog_search" and payload.target_object_id:
        if all(item.id != payload.target_object_id for item in filtered):
            target = next((item for item in payload.dataset.objects if item.id == payload.target_object_id), None)
            if target is not None:
                filtered.append(target)
    return filtered


def _build_ranked_rows(
    *,
    objects: list[Any],
    criteria_by_key: dict[str, CriterionConfig],
    normalized_weights: dict[str, float],
    target_object_id: str | None,
    mode: str,
    dataset_values: dict[str, list[Any]],
) -> list[RankedObject]:
    rows: list[RankedObject] = []
    target_contributions: dict[str, float] | None = None

    for obj in objects:
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
        if target_object_id and obj.id == target_object_id:
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
        if mode == "analog_search" and row.similarity_to_target is not None:
            row.score = row.similarity_to_target
            row.explanation = (
                f"Сходство с целевым объектом: {row.similarity_to_target:.4f}. "
                f"Расчет выполнен по взвешенной близости нормализованных критериев."
            )

    if mode == "analog_search" and target_object_id:
        rows = [row for row in rows if row.object_id != target_object_id]

    rows.sort(
        key=lambda item: (
            item.similarity_to_target if item.similarity_to_target is not None else -1,
            item.score,
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row.rank = index
    return rows


def _scenario_weights(criteria: list[CriterionConfig], variation_pct: float, direction: int) -> dict[str, float]:
    if not criteria:
        return {}
    variation = variation_pct / 100.0
    adjusted: dict[str, float] = {}
    for index, criterion in enumerate(criteria):
        even_index = index % 2 == 0
        if direction == 0:
            factor = 1.0
        elif direction < 0:
            factor = (1.0 - variation) if even_index else (1.0 + variation)
        else:
            factor = (1.0 + variation) if even_index else (1.0 - variation)
        adjusted[criterion.key] = max(0.0001, criterion.weight * factor)
    total = sum(adjusted.values()) or 1.0
    return {key: value / total for key, value in adjusted.items()}


def _build_stability_scenarios(
    *,
    payload: AnalysisRequest,
    objects: list[Any],
    criteria_by_key: dict[str, CriterionConfig],
    baseline_rows: list[RankedObject],
    dataset_values: dict[str, list[Any]],
) -> list[RankingStabilityScenario]:
    if not payload.include_stability_scenarios or not baseline_rows:
        return []

    baseline_top_ids = [row.object_id for row in baseline_rows[: payload.top_n]]
    baseline_positions = {row.object_id: row.rank for row in baseline_rows}
    scenarios: list[tuple[str, int]] = [("-10%", -1), ("Базовый", 0), ("+10%", 1)]
    result: list[RankingStabilityScenario] = []

    for label, direction in scenarios:
        scenario_weights = _scenario_weights(payload.criteria, payload.stability_variation_pct, direction)
        scenario_rows = _build_ranked_rows(
            objects=objects,
            criteria_by_key=criteria_by_key,
            normalized_weights=scenario_weights,
            target_object_id=payload.target_object_id,
            mode=payload.mode,
            dataset_values=dataset_values,
        )
        scenario_top_ids = [row.object_id for row in scenario_rows[: payload.top_n]]
        overlap = len(set(baseline_top_ids) & set(scenario_top_ids))
        changed_positions = sum(
            1
            for obj_id in baseline_top_ids
            if obj_id in {item.object_id for item in scenario_rows}
            and baseline_positions.get(obj_id) != next(
                (item.rank for item in scenario_rows if item.object_id == obj_id),
                None,
            )
        )
        if changed_positions == 0 and overlap == len(baseline_top_ids):
            note = "Состав и порядок топ-N стабильны для сценария."
        elif overlap == 0:
            note = "Состав топ-N полностью изменился в сценарии."
        else:
            note = "Сценарий меняет позиции и/или состав лидеров."

        result.append(
            RankingStabilityScenario(
                label=label,
                variation_pct=payload.stability_variation_pct,
                top_object_id=scenario_rows[0].object_id if scenario_rows else None,
                changed_positions=changed_positions,
                top_n_overlap=overlap,
                note=note,
            )
        )
    return result


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
    objects = _apply_filters(payload)
    if not objects:
        raise ValueError("После применения фильтров не осталось объектов для анализа")

    criteria_by_key = {item.key: item for item in payload.criteria}
    normalized_weights, notes = _weights(payload.criteria, payload.auto_normalize_weights)

    dataset_values = {
        criterion.key: [obj.attributes.get(criterion.key) for obj in objects]
        for criterion in payload.criteria
    }
    rows = _build_ranked_rows(
        objects=objects,
        criteria_by_key=criteria_by_key,
        normalized_weights=normalized_weights,
        target_object_id=payload.target_object_id,
        mode=payload.mode,
        dataset_values=dataset_values,
    )

    if not rows:
        raise ValueError("После расчета не удалось сформировать рейтинг")

    best = rows[0]
    confidence_score, confidence_notes = _confidence(rows, len(payload.criteria))
    scenarios = _build_stability_scenarios(
        payload=payload,
        objects=objects,
        criteria_by_key=criteria_by_key,
        baseline_rows=rows,
        dataset_values=dataset_values,
    )

    source_objects_count = len(payload.dataset.objects)
    if len(objects) != source_objects_count:
        notes.append(f"Фильтры сократили набор объектов: {source_objects_count} -> {len(objects)}")

    return AnalysisResponse(
        summary=AnalysisSummary(
            objects_count=len(objects),
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
            ranking_stability_scenarios=scenarios,
            analog_groups=_analog_groups(rows) if payload.mode == "analog_search" else [],
            dominance_pairs=_dominance_pairs(rows),
        ),
        ranking=rows[: payload.top_n],
    )
