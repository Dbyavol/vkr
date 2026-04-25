from app.services.pipeline_engine import _analysis_dataset, _sanitize_fields_for_preprocessing


def test_analysis_dataset_transformation() -> None:
    rows = [
        {"id": "1", "values": {"price": 100, "area": 50}},
        {"id": "2", "values": {"price": 120, "area": 55}},
    ]

    result = _analysis_dataset(rows)

    assert len(result["objects"]) == 2
    assert result["objects"][0]["id"] == "1"
    assert result["objects"][0]["attributes"]["price"] == 100


def test_sanitize_fields_sets_default_constant_for_numeric() -> None:
    fields = [
        {
            "key": "price",
            "field_type": "numeric",
            "missing_strategy": "constant",
            "missing_constant": None,
        }
    ]

    result = _sanitize_fields_for_preprocessing(fields)

    assert result[0]["missing_constant"] == 0


def test_sanitize_fields_sets_default_constant_for_non_numeric() -> None:
    fields = [
        {
            "key": "district",
            "field_type": "categorical",
            "missing_strategy": "constant",
            "missing_constant": None,
        }
    ]

    result = _sanitize_fields_for_preprocessing(fields)

    assert result[0]["missing_constant"] == "unknown"
