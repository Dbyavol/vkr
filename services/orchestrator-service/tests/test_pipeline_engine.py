from app.services.pipeline_engine import _analysis_dataset


def test_analysis_dataset_transformation() -> None:
    rows = [
        {"id": "1", "values": {"price": 100, "area": 50}},
        {"id": "2", "values": {"price": 120, "area": 55}},
    ]

    result = _analysis_dataset(rows)

    assert len(result["objects"]) == 2
    assert result["objects"][0]["id"] == "1"
    assert result["objects"][0]["attributes"]["price"] == 100
