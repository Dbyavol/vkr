from app.schemas.preprocessing import PreprocessingRequest
from app.services.preprocessing_engine import preprocess_dataset


def test_preprocessing_handles_missing_outliers_and_encoding() -> None:
    payload = PreprocessingRequest.model_validate(
        {
            "dataset": {
                "rows": [
                    {
                        "id": "1",
                        "values": {"price": 100, "area": 45, "condition": "good", "parking": True},
                    },
                    {
                        "id": "2",
                        "values": {"price": 120, "area": 50, "condition": "excellent", "parking": False},
                    },
                    {
                        "id": "3",
                        "values": {"price": 10000, "area": None, "condition": "poor", "parking": True},
                    },
                ]
            },
            "fields": [
                {
                    "key": "price",
                    "field_type": "numeric",
                    "missing_strategy": "median",
                    "outlier_method": "iqr_clip",
                    "normalization": "minmax",
                },
                {
                    "key": "area",
                    "field_type": "numeric",
                    "missing_strategy": "median",
                    "normalization": "zscore",
                },
                {
                    "key": "condition",
                    "field_type": "categorical",
                    "encoding": "ordinal",
                    "ordinal_map": {"poor": 0.2, "good": 0.7, "excellent": 1.0},
                },
                {
                    "key": "parking",
                    "field_type": "binary",
                    "encoding": "binary_map",
                    "binary_map": {"true": 1.0, "false": 0.0},
                },
            ],
            "options": {"preserve_original_values": True},
        }
    )

    result = preprocess_dataset(payload)

    assert result.summary.rows_input == 3
    assert result.summary.rows_output == 3
    assert result.field_reports[0].normalization_applied == "minmax"
    assert result.field_reports[1].rows_missing_before == 1
    assert any(row.original_values is not None for row in result.dataset)
    assert "condition" in result.dataset[0].values


def test_preprocessing_creates_one_hot_columns() -> None:
    payload = PreprocessingRequest.model_validate(
        {
            "dataset": {
                "rows": [
                    {"id": "1", "values": {"kind": "a"}},
                    {"id": "2", "values": {"kind": "b"}},
                ]
            },
            "fields": [
                {
                    "key": "kind",
                    "field_type": "categorical",
                    "encoding": "one_hot",
                }
            ],
        }
    )

    result = preprocess_dataset(payload)
    assert "kind__a" in result.summary.generated_columns
    assert "kind__b" in result.summary.generated_columns


def test_preprocessing_derives_datetime_columns() -> None:
    payload = PreprocessingRequest.model_validate(
        {
            "dataset": {
                "rows": [
                    {"id": "1", "values": {"created_at": "2026-04-19"}},
                    {"id": "2", "values": {"created_at": "2025-12-31 10:20:30"}},
                ]
            },
            "fields": [
                {
                    "key": "created_at",
                    "field_type": "datetime",
                    "include_in_output": False,
                }
            ],
        }
    )

    result = preprocess_dataset(payload)

    assert "created_at__year" in result.summary.generated_columns
    assert "created_at__month" in result.summary.generated_columns
    assert "created_at__day" in result.summary.generated_columns
    assert "created_at__day_of_week" in result.summary.generated_columns
    assert "created_at" not in result.dataset[0].values
    assert result.dataset[0].values["created_at__year"] == 2026.0
