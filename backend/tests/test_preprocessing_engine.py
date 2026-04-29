from app.schemas.preprocessing import (
    DatasetPayload,
    DatasetRow,
    FieldConfig,
    PreprocessingOptions,
    PreprocessingRequest,
)
from app.services.preprocessing_engine import preprocess_dataset


def test_preprocessing_fills_missing_and_normalizes_numeric_field():
    payload = PreprocessingRequest(
        dataset=DatasetPayload(
            rows=[
                DatasetRow(id="1", values={"price": 100, "district": "center"}),
                DatasetRow(id="2", values={"price": None, "district": "north"}),
                DatasetRow(id="3", values={"price": 200, "district": "center"}),
            ]
        ),
        fields=[
            FieldConfig(key="price", field_type="numeric", missing_strategy="median", normalization="minmax"),
            FieldConfig(key="district", field_type="categorical", encoding="one_hot"),
        ],
        options=PreprocessingOptions(drop_duplicate_rows=False),
    )

    result = preprocess_dataset(payload)

    assert result.summary.rows_input == 3
    assert result.summary.rows_output == 3
    assert "district__center" in result.summary.generated_columns
    prices = [row.values["price"] for row in result.dataset]
    assert all(0.0 <= float(price) <= 1.0 for price in prices)


def test_preprocessing_generates_datetime_features():
    payload = PreprocessingRequest(
        dataset=DatasetPayload(
            rows=[
                DatasetRow(id="1", values={"posted_at": "2026-04-01"}),
                DatasetRow(id="2", values={"posted_at": "2026-04-02"}),
            ]
        ),
        fields=[FieldConfig(key="posted_at", field_type="datetime")],
    )

    result = preprocess_dataset(payload)

    assert "posted_at__year" in result.summary.generated_columns
    assert result.dataset[0].values["posted_at__month"] == 4.0
