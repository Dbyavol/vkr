from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


CriterionType = Literal["numeric", "categorical", "binary"]
DirectionType = Literal["maximize", "minimize", "target"]
AnalysisMode = Literal["rating", "analog_search"]


class DatasetObject(BaseModel):
    id: str
    title: str
    attributes: dict[str, Any]


class CriterionConfig(BaseModel):
    key: str
    name: str
    weight: float = Field(ge=0)
    type: CriterionType = "numeric"
    direction: DirectionType = "maximize"
    scale_map: dict[str, float] | None = None
    target_value: Any | None = None

    @model_validator(mode="after")
    def validate_target_mode(self) -> "CriterionConfig":
        if self.direction == "target" and self.target_value is None:
            raise ValueError("criterion.target_value is required when direction=target")
        return self


class AnalysisDataset(BaseModel):
    objects: list[DatasetObject]


class AnalysisRequest(BaseModel):
    dataset: AnalysisDataset
    criteria: list[CriterionConfig]
    target_object_id: str | None = None
    mode: AnalysisMode = "rating"
    top_n: int = Field(default=10, ge=1)
    auto_normalize_weights: bool = True
    include_explanations: bool = True

    @model_validator(mode="after")
    def validate_payload(self) -> "AnalysisRequest":
        if not self.dataset.objects:
            raise ValueError("dataset.objects must contain at least one object")
        if not self.criteria:
            raise ValueError("criteria must contain at least one criterion")
        return self


class CriterionContribution(BaseModel):
    key: str
    name: str
    raw_value: Any
    normalized_value: float
    weight: float
    contribution: float
    note: str | None = None


class RankedObject(BaseModel):
    object_id: str
    title: str
    rank: int
    score: float
    similarity_to_target: float | None = None
    contributions: list[CriterionContribution]
    explanation: str


class CriterionSensitivity(BaseModel):
    key: str
    name: str
    weight: float
    normalized_range: float
    sensitivity_index: float
    note: str


class AnalogGroup(BaseModel):
    label: str
    min_score: float
    max_score: float
    object_ids: list[str]


class DominancePair(BaseModel):
    dominant_object_id: str
    dominated_object_id: str
    criteria_count: int


class AnalysisSummary(BaseModel):
    objects_count: int
    criteria_count: int
    weights_sum: float
    best_object_id: str
    best_score: float
    normalization_notes: list[str]
    mode: str = "rating"
    target_object_id: str | None = None
    confidence_score: float = 1.0
    confidence_notes: list[str] = []
    sensitivity: list[CriterionSensitivity] = []
    ranking_stability_note: str | None = None
    analog_groups: list[AnalogGroup] = []
    dominance_pairs: list[DominancePair] = []


class AnalysisResponse(BaseModel):
    summary: AnalysisSummary
    ranking: list[RankedObject]
