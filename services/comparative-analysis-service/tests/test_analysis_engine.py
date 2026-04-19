from app.schemas.analysis import AnalysisRequest
from app.services.analysis_engine import run_comparative_analysis


def test_analysis_ranks_objects_by_weighted_score() -> None:
    payload = AnalysisRequest.model_validate(
        {
            "dataset": {
                "objects": [
                    {
                        "id": "flat-1",
                        "title": "Object 1",
                        "attributes": {"price": 100, "area": 50, "condition": "good"},
                    },
                    {
                        "id": "flat-2",
                        "title": "Object 2",
                        "attributes": {"price": 120, "area": 65, "condition": "excellent"},
                    },
                ]
            },
            "criteria": [
                {"key": "price", "name": "Price", "weight": 0.4, "type": "numeric", "direction": "minimize"},
                {"key": "area", "name": "Area", "weight": 0.4, "type": "numeric", "direction": "maximize"},
                {
                    "key": "condition",
                    "name": "Condition",
                    "weight": 0.2,
                    "type": "categorical",
                    "direction": "maximize",
                    "scale_map": {"good": 0.7, "excellent": 1.0},
                },
            ],
            "top_n": 5,
        }
    )

    result = run_comparative_analysis(payload)

    assert result.summary.objects_count == 2
    assert result.summary.criteria_count == 3
    assert result.ranking[0].object_id == "flat-2"
    assert result.ranking[0].rank == 1
    assert result.ranking[0].score > result.ranking[1].score


def test_analysis_normalizes_weights_automatically() -> None:
    payload = AnalysisRequest.model_validate(
        {
            "dataset": {
                "objects": [
                    {"id": "a", "title": "A", "attributes": {"cost": 10}},
                    {"id": "b", "title": "B", "attributes": {"cost": 20}},
                ]
            },
            "criteria": [
                {"key": "cost", "name": "Cost", "weight": 40, "type": "numeric", "direction": "minimize"},
            ],
        }
    )
    result = run_comparative_analysis(payload)
    assert result.summary.weights_sum == 1.0
    assert result.summary.normalization_notes


def test_analysis_can_search_analogs_for_target_object() -> None:
    payload = AnalysisRequest.model_validate(
        {
            "mode": "analog_search",
            "target_object_id": "target",
            "dataset": {
                "objects": [
                    {"id": "target", "title": "Target", "attributes": {"price": 100, "area": 50}},
                    {"id": "close", "title": "Close", "attributes": {"price": 102, "area": 51}},
                    {"id": "far", "title": "Far", "attributes": {"price": 200, "area": 90}},
                ]
            },
            "criteria": [
                {"key": "price", "name": "Price", "weight": 0.5, "type": "numeric", "direction": "minimize"},
                {"key": "area", "name": "Area", "weight": 0.5, "type": "numeric", "direction": "maximize"},
            ],
        }
    )

    result = run_comparative_analysis(payload)

    assert result.summary.mode == "analog_search"
    assert result.ranking[0].object_id == "close"
    assert all(item.object_id != "target" for item in result.ranking)


def test_analysis_parses_numeric_strings_from_imported_csv() -> None:
    payload = AnalysisRequest.model_validate(
        {
            "dataset": {
                "objects": [
                    {"id": "a", "title": "A", "attributes": {"price": "120000", "rating": "4,7"}},
                    {"id": "b", "title": "B", "attributes": {"price": "98000", "rating": "4.2"}},
                    {"id": "c", "title": "C", "attributes": {"price": "", "rating": "bad-value"}},
                ]
            },
            "criteria": [
                {"key": "price", "name": "Price", "weight": 0.5, "type": "numeric", "direction": "minimize"},
                {"key": "rating", "name": "Rating", "weight": 0.5, "type": "numeric", "direction": "maximize"},
            ],
        }
    )

    result = run_comparative_analysis(payload)

    assert result.ranking[0].object_id in {"a", "b"}
    assert result.ranking[0].score > result.ranking[-1].score
    assert "В датасете нет числовых значений по этому критерию" not in {
        contribution.note
        for row in result.ranking
        for contribution in row.contributions
        if contribution.note
    }


def test_analysis_applies_server_side_numeric_filters() -> None:
    payload = AnalysisRequest.model_validate(
        {
            "mode": "analog_search",
            "target_object_id": "target",
            "dataset": {
                "objects": [
                    {"id": "target", "title": "Target", "attributes": {"price": 100, "area": 50}},
                    {"id": "a", "title": "A", "attributes": {"price": 103, "area": 52}},
                    {"id": "b", "title": "B", "attributes": {"price": 240, "area": 90}},
                ]
            },
            "criteria": [
                {"key": "price", "name": "Price", "weight": 0.5, "type": "numeric", "direction": "minimize"},
                {"key": "area", "name": "Area", "weight": 0.5, "type": "numeric", "direction": "maximize"},
            ],
            "filter_criteria": {
                "numeric_ranges": [
                    {"key": "price", "max_value": 150},
                ]
            },
        }
    )

    result = run_comparative_analysis(payload)

    assert len(result.ranking) == 1
    assert result.ranking[0].object_id == "a"
    assert any("Фильтры сократили набор объектов" in note for note in result.summary.normalization_notes)


def test_analysis_returns_stability_scenarios() -> None:
    payload = AnalysisRequest.model_validate(
        {
            "dataset": {
                "objects": [
                    {"id": "o1", "title": "One", "attributes": {"price": 100, "area": 50, "floor": 5}},
                    {"id": "o2", "title": "Two", "attributes": {"price": 120, "area": 62, "floor": 3}},
                    {"id": "o3", "title": "Three", "attributes": {"price": 95, "area": 47, "floor": 9}},
                ]
            },
            "criteria": [
                {"key": "price", "name": "Price", "weight": 0.45, "type": "numeric", "direction": "minimize"},
                {"key": "area", "name": "Area", "weight": 0.35, "type": "numeric", "direction": "maximize"},
                {"key": "floor", "name": "Floor", "weight": 0.2, "type": "numeric", "direction": "maximize"},
            ],
            "include_stability_scenarios": True,
            "stability_variation_pct": 10,
            "top_n": 3,
        }
    )

    result = run_comparative_analysis(payload)

    assert len(result.summary.ranking_stability_scenarios) == 3
    assert {scenario.label for scenario in result.summary.ranking_stability_scenarios} == {"-10%", "Базовый", "+10%"}
