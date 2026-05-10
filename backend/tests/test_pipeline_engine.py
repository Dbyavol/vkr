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


def test_pipeline_engine_analog_search_with_geo_radius_filters_rows(sample_geo_csv_bytes: bytes):
    payload = PipelineRequest(
        filename="geo.csv",
        config=PipelineConfig(
            fields=[
                FieldConfig(key="name", field_type="text"),
                FieldConfig(key="price", field_type="float", normalization="minmax"),
                FieldConfig(key="area", field_type="float", normalization="minmax"),
                FieldConfig(key="lat", field_type="geo_latitude", normalization="none"),
                FieldConfig(key="lon", field_type="geo_longitude", normalization="none"),
            ],
            criteria=[
                CriterionConfig(key="price", name="Цена", weight=0.5, type="numeric", direction="target"),
                CriterionConfig(key="area", name="Площадь", weight=0.5, type="numeric", direction="target"),
            ],
            analysis_mode="analog_search",
            target_row_id="1",
            geo_radius_km=3,
        ),
    )

    result = asyncio.run(run_pipeline_via_services(filename="geo.csv", body=sample_geo_csv_bytes, payload=payload))

    assert result.analysis_summary["mode"] == "analog_search"
    assert result.analysis_summary["objects_count"] == 2
    assert len(result.ranking) == 1
    assert {item.object_id for item in result.ranking} == {"2"}


def test_pipeline_engine_geo_radius_still_works_when_geo_fields_hidden(sample_geo_csv_bytes: bytes):
    payload = PipelineRequest(
        filename="geo.csv",
        config=PipelineConfig(
            fields=[
                FieldConfig(key="name", field_type="text"),
                FieldConfig(key="price", field_type="float", normalization="minmax"),
                FieldConfig(key="area", field_type="float", normalization="minmax"),
                FieldConfig(key="lat", field_type="geo_latitude", normalization="none", include_in_output=False),
                FieldConfig(key="lon", field_type="geo_longitude", normalization="none", include_in_output=False),
            ],
            criteria=[
                CriterionConfig(key="price", name="Р¦РµРЅР°", weight=0.5, type="numeric", direction="target"),
                CriterionConfig(key="area", name="РџР»РѕС‰Р°РґСЊ", weight=0.5, type="numeric", direction="target"),
            ],
            analysis_mode="analog_search",
            target_row_id="1",
            geo_radius_km=3,
        ),
    )

    result = asyncio.run(run_pipeline_via_services(filename="geo.csv", body=sample_geo_csv_bytes, payload=payload))

    assert result.analysis_summary["mode"] == "analog_search"
    assert result.analysis_summary["objects_count"] == 2
    assert len(result.ranking) == 1
    assert {item.object_id for item in result.ranking} == {"2"}


def test_pipeline_engine_market_valuation_for_analogs(sample_geo_csv_bytes: bytes):
    payload = PipelineRequest(
        filename="geo.csv",
        config=PipelineConfig(
            fields=[
                FieldConfig(key="name", field_type="text"),
                FieldConfig(key="price", field_type="float", normalization="minmax"),
                FieldConfig(key="area", field_type="float", normalization="minmax"),
                FieldConfig(key="lat", field_type="geo_latitude", normalization="none", include_in_output=False),
                FieldConfig(key="lon", field_type="geo_longitude", normalization="none", include_in_output=False),
            ],
            criteria=[
                CriterionConfig(key="price", name="Цена", weight=0.5, type="numeric", direction="target"),
                CriterionConfig(key="area", name="Площадь", weight=0.5, type="numeric", direction="target"),
            ],
            analysis_mode="analog_search",
            target_row_id="1",
            geo_radius_km=3,
            top_n=1,
            enable_market_valuation=True,
            valuation_price_field_key="price",
            valuation_analogs_count=3,
        ),
    )

    result = asyncio.run(run_pipeline_via_services(filename="geo.csv", body=sample_geo_csv_bytes, payload=payload))

    valuation = result.analysis_summary.get("market_valuation")
    assert valuation is not None
    assert valuation["price_field_key"] == "price"
    assert valuation["analogs_used"] == 1
    assert valuation["estimated_price"] == 105.0
    assert valuation["target_price"] == 100.0
    assert result.analysis_summary["criteria_count"] == 1
    assert {item.key for item in result.ranking[0].contributions} == {"area"}
