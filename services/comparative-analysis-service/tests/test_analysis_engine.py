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
