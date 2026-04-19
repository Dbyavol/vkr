from app.schemas.preprocessing import DatasetProfileRequest
from app.services.profiling_engine import profile_dataset


def test_profile_dataset_recommends_numeric_and_categorical_processing() -> None:
    payload = DatasetProfileRequest.model_validate(
        {
            "dataset": {
                "rows": [
                    {"id": "1", "values": {"price": "100", "category": "premium", "comment": "A"}},
                    {"id": "2", "values": {"price": "120", "category": "standard", "comment": "B"}},
                    {"id": "3", "values": {"price": "9999", "category": "premium", "comment": "C"}},
                    {"id": "4", "values": {"price": "", "category": "budget", "comment": "D"}},
                ]
            }
        }
    )

    result = profile_dataset(payload)
    fields = {field.key: field for field in result.fields}

    assert fields["price"].inferred_type == "numeric"
    assert fields["price"].histogram
    assert any(item.code == "NORMALIZE_NUMERIC" for item in fields["price"].recommendations)
    assert fields["category"].inferred_type == "categorical"
    assert fields["category"].top_categories
    assert result.quality.score < 100
    assert result.quality.total_missing_values == 1
    assert any(item.code == "MISSING_VALUES" for item in result.quality.issues)


def test_profile_dataset_detects_datetime_fields() -> None:
    payload = DatasetProfileRequest.model_validate(
        {
            "dataset": {
                "rows": [
                    {"id": "1", "values": {"created_at": "2026-04-19"}},
                    {"id": "2", "values": {"created_at": "2026-04-20"}},
                    {"id": "3", "values": {"created_at": "2026-04-21 12:00:00"}},
                ]
            }
        }
    )

    result = profile_dataset(payload)
    field = next(item for item in result.fields if item.key == "created_at")

    assert field.inferred_type == "datetime"
    assert field.recommended_config.include_in_output is False
    assert any(item.code == "DERIVE_DATETIME_FEATURES" for item in field.recommendations)
