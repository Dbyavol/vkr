from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

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


def _to_numeric_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(float)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    normalized = (
        series.astype("string")
        .str.strip()
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace({"": pd.NA})
    )
    return pd.to_numeric(normalized, errors="coerce")


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


def _ranking_stability_note(sorted_scores: list[float]) -> str:
    if len(sorted_scores) < 2:
        return "Доступен только один объект сравнения, устойчивость рейтинга оценить нельзя."
    margin = sorted_scores[0] - sorted_scores[1]
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

    if payload.mode == "analog_search" and payload.target_object_id:
        if all(item.id != payload.target_object_id for item in filtered):
            target = next((item for item in payload.dataset.objects if item.id == payload.target_object_id), None)
            if target is not None:
                filtered.append(target)
    return filtered


def _build_objects_frame(objects: list[Any], criteria: list[CriterionConfig]) -> tuple[pd.DataFrame, dict[str, str]]:
    records: list[dict[str, Any]] = []
    title_by_id: dict[str, str] = {}
    for obj in objects:
        object_id = str(obj.id)
        title_by_id[object_id] = obj.title
        record = {"__id": object_id, "__title": obj.title}
        for criterion in criteria:
            record[criterion.key] = obj.attributes.get(criterion.key)
        records.append(record)
    frame = pd.DataFrame(records).set_index("__id", drop=True)
    return frame, title_by_id


def _normalize_criteria_frame(
    frame: pd.DataFrame,
    criteria: list[CriterionConfig],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized = pd.DataFrame(index=frame.index)
    notes = pd.DataFrame(index=frame.index)

    for criterion in criteria:
        key = criterion.key
        raw = frame[key] if key in frame.columns else pd.Series(index=frame.index, dtype="object")

        if criterion.type == "numeric":
            numeric = _to_numeric_series(raw)
            valid = numeric.dropna()
            result = pd.Series(0.0, index=frame.index, dtype=float)
            note_series = pd.Series([None] * len(frame.index), index=frame.index, dtype="object")
            missing_mask = numeric.isna()
            if missing_mask.any():
                note_series.loc[missing_mask] = "Значение отсутствует или не является числом"
            if valid.empty:
                note_series.loc[:] = "В датасете нет числовых значений по этому критерию"
            else:
                min_value = float(valid.min())
                max_value = float(valid.max())
                valid_mask = numeric.notna()
                if min_value == max_value:
                    result.loc[valid_mask] = 1.0
                    note_series.loc[valid_mask] = "У всех объектов одинаковое значение по этому критерию"
                else:
                    if criterion.direction == "minimize":
                        result.loc[valid_mask] = (max_value - numeric.loc[valid_mask]) / (max_value - min_value)
                    else:
                        result.loc[valid_mask] = (numeric.loc[valid_mask] - min_value) / (max_value - min_value)
            normalized[key] = result.fillna(0.0).round(4)
            notes[key] = note_series
            continue

        raw_as_string = raw.astype("string")
        if criterion.direction == "target":
            normalized[key] = raw.eq(criterion.target_value).astype(float).round(4)
            notes[key] = pd.Series([None] * len(frame.index), index=frame.index, dtype="object")
            continue

        if criterion.scale_map:
            mapped = raw_as_string.map(lambda value: criterion.scale_map.get(str(value), 0.0) if pd.notna(value) else 0.0)
            normalized[key] = pd.to_numeric(mapped, errors="coerce").fillna(0.0).round(4)
            notes[key] = pd.Series([None] * len(frame.index), index=frame.index, dtype="object")
            continue

        if pd.api.types.is_bool_dtype(raw):
            normalized[key] = raw.astype(float).fillna(0.0).round(4)
            notes[key] = pd.Series([None] * len(frame.index), index=frame.index, dtype="object")
            continue

        normalized[key] = pd.Series(0.0, index=frame.index, dtype=float)
        notes[key] = pd.Series(
            ["Для категориального критерия не задана шкала"] * len(frame.index),
            index=frame.index,
            dtype="object",
        )

    return normalized, notes


def _criterion_sensitivity(
    normalized_df: pd.DataFrame,
    normalized_weights: dict[str, float],
    criteria: list[CriterionConfig],
) -> list[CriterionSensitivity]:
    result: list[CriterionSensitivity] = []
    for criterion in criteria:
        key = criterion.key
        if key not in normalized_df.columns:
            continue
        values = normalized_df[key]
        if values.empty:
            continue
        normalized_range = float(values.max() - values.min())
        weight = float(normalized_weights.get(key, 0.0))
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


def _score_frame(
    normalized_df: pd.DataFrame,
    weights: dict[str, float],
    mode: str,
    target_object_id: str | None,
) -> tuple[pd.Series, pd.Series | None]:
    weight_series = pd.Series(weights, dtype=float)
    contributions_df = normalized_df.mul(weight_series, axis=1)
    if mode != "analog_search":
        return contributions_df.sum(axis=1).round(4), None

    if target_object_id is None or target_object_id not in normalized_df.index:
        return contributions_df.sum(axis=1).round(4), None

    target_vector = normalized_df.loc[str(target_object_id)]
    similarity_df = 1.0 - (normalized_df.sub(target_vector, axis=1).abs())
    similarity_df = similarity_df.clip(lower=0.0)
    weighted_similarity = similarity_df.mul(weight_series, axis=1)
    total_weight = float(weight_series.sum()) or 1.0
    scores = (weighted_similarity.sum(axis=1) / total_weight).round(4)
    return scores, scores.copy()


def _build_stability_scenarios(
    *,
    payload: AnalysisRequest,
    normalized_df: pd.DataFrame,
    baseline_ranked_ids: list[str],
) -> list[RankingStabilityScenario]:
    if not payload.include_stability_scenarios or not baseline_ranked_ids:
        return []

    baseline_top_ids = baseline_ranked_ids[: payload.top_n]
    baseline_positions = {object_id: index for index, object_id in enumerate(baseline_ranked_ids, start=1)}
    scenarios: list[tuple[str, int]] = [("-10%", -1), ("Базовый", 0), ("+10%", 1)]
    result: list[RankingStabilityScenario] = []

    for label, direction in scenarios:
        variation = payload.stability_variation_pct / 100.0
        adjusted: dict[str, float] = {}
        for index, criterion in enumerate(payload.criteria):
            even_index = index % 2 == 0
            if direction == 0:
                factor = 1.0
            elif direction < 0:
                factor = (1.0 - variation) if even_index else (1.0 + variation)
            else:
                factor = (1.0 + variation) if even_index else (1.0 - variation)
            adjusted[criterion.key] = max(0.0001, criterion.weight * factor)
        total = sum(adjusted.values()) or 1.0
        normalized_weights = {key: value / total for key, value in adjusted.items()}
        scenario_scores, scenario_similarity = _score_frame(
            normalized_df=normalized_df,
            weights=normalized_weights,
            mode=payload.mode,
            target_object_id=payload.target_object_id,
        )
        scenario_order = pd.DataFrame({"score": scenario_scores})
        if payload.mode == "analog_search" and scenario_similarity is not None:
            scenario_order["similarity"] = scenario_similarity
            scenario_order = scenario_order.drop(index=[payload.target_object_id], errors="ignore")
            scenario_order = scenario_order.sort_values(by=["similarity", "score"], ascending=False)
        else:
            scenario_order = scenario_order.sort_values(by=["score"], ascending=False)
        scenario_ids = scenario_order.index.tolist()
        scenario_top_ids = scenario_ids[: payload.top_n]
        overlap = len(set(baseline_top_ids) & set(scenario_top_ids))
        scenario_positions = {object_id: index for index, object_id in enumerate(scenario_ids, start=1)}
        changed_positions = sum(
            1
            for object_id in baseline_top_ids
            if object_id in scenario_positions and baseline_positions.get(object_id) != scenario_positions.get(object_id)
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
                top_object_id=scenario_ids[0] if scenario_ids else None,
                changed_positions=changed_positions,
                top_n_overlap=overlap,
                note=note,
            )
        )
    return result


def _analog_groups(score_series: pd.Series) -> list[AnalogGroup]:
    groups = [
        ("Очень близкие аналоги", 0.85, 1.0),
        ("Умеренно близкие аналоги", 0.65, 0.8499),
        ("Слабые аналоги", 0.0, 0.6499),
    ]
    result: list[AnalogGroup] = []
    for label, low, high in groups:
        ids = score_series[(score_series >= low) & (score_series <= high)].index.tolist()
        if ids:
            result.append(AnalogGroup(label=label, min_score=low, max_score=high, object_ids=ids))
    return result


def _dominance_pairs(normalized_df: pd.DataFrame, ranked_ids: list[str], limit: int = 8) -> list[DominancePair]:
    pairs: list[DominancePair] = []
    matrix = normalized_df.loc[ranked_ids].fillna(0.0)
    for left_id in matrix.index:
        left = matrix.loc[left_id]
        for right_id in matrix.index:
            if left_id == right_id:
                continue
            right = matrix.loc[right_id]
            comparison = left >= right
            if bool(comparison.all()) and bool((left > right).any()):
                pairs.append(
                    DominancePair(
                        dominant_object_id=str(left_id),
                        dominated_object_id=str(right_id),
                        criteria_count=int(len(matrix.columns)),
                    )
                )
            if len(pairs) >= limit:
                return pairs
    return pairs


def _confidence(
    notes_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    criteria_count: int,
) -> tuple[float, list[str]]:
    notes: list[str] = []
    flat_mask = notes_df.apply(
        lambda column: column.map(lambda value: isinstance(value, str) and "одинаковое значение" in value)
    )
    missing_mask = notes_df.apply(
        lambda column: column.map(
            lambda value: isinstance(value, str)
            and (
                "отсутствует" in value
                or "не является числом" in value
                or "нет числовых" in value
                or "не задана шкала" in value
            )
        )
    )
    missing_notes = int(missing_mask.sum().sum())
    total_cells = max(len(normalized_df.index) * criteria_count, 1)
    missing_ratio = missing_notes / total_cells
    confidence = 1.0 - min(0.55, missing_ratio)
    flat_criteria = {column for column in notes_df.columns if bool(flat_mask[column].any())}
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


def _build_ranking(
    *,
    frame: pd.DataFrame,
    normalized_df: pd.DataFrame,
    notes_df: pd.DataFrame,
    score_series: pd.Series,
    similarity_series: pd.Series | None,
    criteria: list[CriterionConfig],
    weights: dict[str, float],
    mode: str,
    target_object_id: str | None,
) -> tuple[list[RankedObject], list[str]]:
    ranking_frame = pd.DataFrame(
        {
            "title": frame["__title"],
            "score": score_series,
        },
        index=frame.index,
    )
    if mode == "analog_search" and similarity_series is not None:
        ranking_frame["similarity"] = similarity_series
        ranking_frame = ranking_frame.drop(index=[target_object_id], errors="ignore")
        ranking_frame = ranking_frame.sort_values(by=["similarity", "score"], ascending=False)
    else:
        ranking_frame = ranking_frame.sort_values(by=["score"], ascending=False)

    ranking: list[RankedObject] = []
    ordered_ids = ranking_frame.index.tolist()
    for rank, object_id in enumerate(ordered_ids, start=1):
        contributions: list[CriterionContribution] = []
        for criterion in criteria:
            key = criterion.key
            raw_value = frame.at[object_id, key]
            note = notes_df.at[object_id, key] if key in notes_df.columns else None
            normalized_value = float(normalized_df.at[object_id, key]) if key in normalized_df.columns else 0.0
            weight = float(weights.get(key, 0.0))
            contribution = round(normalized_value * weight, 4)
            contributions.append(
                CriterionContribution(
                    key=key,
                    name=criterion.name,
                    raw_value=None if pd.isna(raw_value) else raw_value,
                    normalized_value=round(normalized_value, 4),
                    weight=round(weight, 4),
                    contribution=contribution,
                    note=note if isinstance(note, str) else None,
                )
            )
        if mode == "analog_search" and similarity_series is not None:
            similarity = float(similarity_series.loc[object_id])
            explanation = (
                f"Сходство с целевым объектом: {similarity:.4f}. "
                "Расчет выполнен по взвешенной близости нормализованных критериев."
            )
            score = similarity
        else:
            similarity = None
            top_positive = sorted(contributions, key=lambda item: item.contribution, reverse=True)[:3]
            top_negative = sorted(contributions, key=lambda item: item.contribution)[:2]
            explanation = (
                f"Сильнее всего оценку повысили: {', '.join(item.name for item in top_positive) or 'нет данных'}. "
                f"Слабее всего повлияли: {', '.join(item.name for item in top_negative) or 'нет данных'}."
            )
            score = float(score_series.loc[object_id])
        ranking.append(
            RankedObject(
                object_id=str(object_id),
                title=str(ranking_frame.at[object_id, 'title']),
                rank=rank,
                score=round(score, 4),
                similarity_to_target=round(similarity, 4) if similarity is not None else None,
                contributions=contributions,
                explanation=explanation,
            )
        )
    return ranking, ordered_ids


def run_comparative_analysis(payload: AnalysisRequest) -> AnalysisResponse:
    objects = _apply_filters(payload)
    if not objects:
        raise ValueError("После применения фильтров не осталось объектов для анализа")

    normalized_weights, notes = _weights(payload.criteria, payload.auto_normalize_weights)
    frame, _ = _build_objects_frame(objects, payload.criteria)
    normalized_df, notes_df = _normalize_criteria_frame(frame, payload.criteria)
    score_series, similarity_series = _score_frame(
        normalized_df=normalized_df,
        weights=normalized_weights,
        mode=payload.mode,
        target_object_id=payload.target_object_id,
    )
    rows, ranked_ids = _build_ranking(
        frame=frame,
        normalized_df=normalized_df,
        notes_df=notes_df,
        score_series=score_series,
        similarity_series=similarity_series,
        criteria=payload.criteria,
        weights=normalized_weights,
        mode=payload.mode,
        target_object_id=payload.target_object_id,
    )
    if not rows:
        raise ValueError("После расчета не удалось сформировать рейтинг")

    best = rows[0]
    confidence_score, confidence_notes = _confidence(notes_df.loc[ranked_ids], normalized_df.loc[ranked_ids], len(payload.criteria))
    scenarios = _build_stability_scenarios(
        payload=payload,
        normalized_df=normalized_df,
        baseline_ranked_ids=ranked_ids,
    )

    source_objects_count = len(payload.dataset.objects)
    if len(objects) != source_objects_count:
        notes.append(f"Фильтры сократили набор объектов: {source_objects_count} -> {len(objects)}")

    ranked_scores = [row.score for row in rows]
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
            sensitivity=_criterion_sensitivity(normalized_df.loc[ranked_ids], normalized_weights, payload.criteria),
            ranking_stability_note=_ranking_stability_note(ranked_scores),
            ranking_stability_scenarios=scenarios,
            analog_groups=_analog_groups(pd.Series(ranked_scores, index=[row.object_id for row in rows])) if payload.mode == "analog_search" else [],
            dominance_pairs=_dominance_pairs(normalized_df, ranked_ids),
        ),
        ranking=rows[: payload.top_n],
    )
