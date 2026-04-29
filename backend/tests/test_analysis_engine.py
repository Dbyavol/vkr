from app.schemas.analysis import AnalysisDataset, AnalysisRequest, CriterionConfig, DatasetObject
from app.services.analysis_engine import run_comparative_analysis


def test_analysis_engine_builds_rating():
    payload = AnalysisRequest(
        dataset=AnalysisDataset(
            objects=[
                DatasetObject(id="1", title="A", attributes={"price": 100, "area": 50}),
                DatasetObject(id="2", title="B", attributes={"price": 90, "area": 45}),
                DatasetObject(id="3", title="C", attributes={"price": 120, "area": 60}),
            ]
        ),
        criteria=[
            CriterionConfig(key="price", name="Цена", weight=0.6, type="numeric", direction="minimize"),
            CriterionConfig(key="area", name="Площадь", weight=0.4, type="numeric", direction="maximize"),
        ],
    )

    result = run_comparative_analysis(payload)

    assert result.summary.objects_count == 3
    assert len(result.ranking) == 3
    assert result.ranking[0].rank == 1


def test_analysis_engine_supports_analog_search():
    payload = AnalysisRequest(
        dataset=AnalysisDataset(
            objects=[
                DatasetObject(id="1", title="Target", attributes={"price": 100, "area": 50}),
                DatasetObject(id="2", title="Near", attributes={"price": 102, "area": 51}),
                DatasetObject(id="3", title="Far", attributes={"price": 150, "area": 80}),
            ]
        ),
        criteria=[
            CriterionConfig(key="price", name="Цена", weight=0.5, type="numeric", direction="target", target_value=100),
            CriterionConfig(key="area", name="Площадь", weight=0.5, type="numeric", direction="target", target_value=50),
        ],
        target_object_id="1",
        mode="analog_search",
    )

    result = run_comparative_analysis(payload)

    assert result.summary.mode == "analog_search"
    assert result.ranking[0].object_id == "2"
    assert all(item.object_id != "1" for item in result.ranking)
