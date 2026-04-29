from app.schemas.preprocessing import DatasetPayload, DatasetProfileRequest, DatasetRow
from app.services.profiling_engine import profile_dataset


def test_profile_dataset_returns_quality_and_recommendations():
    payload = DatasetProfileRequest(
        dataset=DatasetPayload(
            rows=[
                DatasetRow(id="1", values={"price": 100, "district": "center", "posted_at": "2026-04-01"}),
                DatasetRow(id="2", values={"price": 120, "district": "north", "posted_at": "2026-04-02"}),
                DatasetRow(id="3", values={"price": None, "district": "center", "posted_at": "2026-04-03"}),
            ]
        )
    )

    profile = profile_dataset(payload)

    assert profile.rows_total == 3
    assert profile.quality.score <= 100
    assert any(field.key == "price" and field.inferred_type == "numeric" for field in profile.fields)
    assert any(field.key == "district" and field.inferred_type == "categorical" for field in profile.fields)
    assert "price" in profile.recommended_weights
