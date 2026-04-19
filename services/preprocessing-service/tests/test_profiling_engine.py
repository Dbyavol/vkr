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
