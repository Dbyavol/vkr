import asyncio

import pytest

from app.services.pipeline_engine import upload_dataset_file
from app.services.profile_artifact_service import (
    build_and_cache_detailed_profile_artifact,
    build_detailed_profile_artifact_key,
    load_cached_detailed_profile,
)


def test_profile_artifact_build_and_load(sample_csv_bytes: bytes):
    uploaded = asyncio.run(upload_dataset_file(filename="dataset.csv", body=sample_csv_bytes))
    dataset_file_id = int(uploaded["id"])

    profile = build_and_cache_detailed_profile_artifact(dataset_file_id=dataset_file_id, filename="dataset.csv")
    cached = load_cached_detailed_profile(dataset_file_id)

    assert profile["rows_total"] == 3
    assert cached is not None
    assert cached["rows_total"] == 3
    assert isinstance(build_detailed_profile_artifact_key(dataset_file_id), str)


def test_profile_artifact_missing_dataset_raises():
    with pytest.raises(ValueError, match="Файл датасета не найден"):
        build_and_cache_detailed_profile_artifact(dataset_file_id=999999, filename="missing.csv")
