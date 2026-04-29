import asyncio

from app.schemas.pipeline import CriterionConfig, FieldConfig, PipelineConfig, PipelineRequest
from app.services.pipeline_engine import refresh_preprocessing_from_storage, run_pipeline_via_services, upload_and_profile_dataset


def test_pipeline_engine_runs_end_to_end(sample_csv_bytes: bytes):
    payload = PipelineRequest(
        filename="dataset.csv",
        config=PipelineConfig(
            fields=[
                FieldConfig(key="price", field_type="numeric", normalization="minmax"),
                FieldConfig(key="area", field_type="numeric", normalization="minmax"),
                FieldConfig(key="rooms", field_type="numeric", normalization="minmax"),
                FieldConfig(key="condition", field_type="categorical", encoding="ordinal", ordinal_map={"old": 0.2, "good": 0.7, "new": 1.0}),
                FieldConfig(key="district", field_type="categorical", encoding="one_hot"),
            ],
            criteria=[
                CriterionConfig(key="price", name="Цена", weight=0.5, type="numeric", direction="minimize"),
                CriterionConfig(key="area", name="Площадь", weight=0.3, type="numeric", direction="maximize"),
                CriterionConfig(key="rooms", name="Комнаты", weight=0.2, type="numeric", direction="maximize"),
            ],
        ),
    )

    result = asyncio.run(run_pipeline_via_services(filename="dataset.csv", body=sample_csv_bytes, payload=payload))

    assert result.analysis_summary["objects_count"] == 3
    assert result.history_id is None
    assert len(result.ranking) == 3


def test_pipeline_refresh_from_stored_file(sample_csv_bytes: bytes):
    uploaded = asyncio.run(upload_and_profile_dataset(filename="dataset.csv", body=sample_csv_bytes))

    refreshed = asyncio.run(
        refresh_preprocessing_from_storage(
            dataset_file_id=uploaded["dataset_file_id"],
            filename="dataset.csv",
            fields=[
                {
                    "key": "price",
                    "field_type": "numeric",
                    "normalization": "minmax",
                    "missing_strategy": "none",
                    "outlier_method": "none",
                    "encoding": "none",
                }
            ],
        )
    )

    assert refreshed["preview"]["rows_total"] == 3
    assert "profile" in refreshed
