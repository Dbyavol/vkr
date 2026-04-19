from typing import Any, Literal

from pydantic import BaseModel, Field


class PipelineRow(BaseModel):
    id: str
    values: dict[str, Any]


class ImportedColumn(BaseModel):
    source_name: str
    normalized_name: str
    inferred_type: str
    missing_count: int
    unique_count: int
    sample_values: list[Any]


class ImportedPreview(BaseModel):
    filename: str
    rows_total: int
    columns: list[ImportedColumn]
    preview_rows: list[dict[str, Any]]
    warnings: list[str]
    normalized_dataset: dict[str, list[PipelineRow]]


FieldType = Literal["numeric", "categorical", "binary", "text"]
MissingStrategy = Literal["none", "drop_row", "mean", "median", "mode", "constant"]
OutlierMethod = Literal["none", "iqr_remove", "iqr_clip", "zscore_remove", "zscore_clip"]
NormalizationMethod = Literal["none", "minmax", "zscore", "robust", "log_minmax"]
EncodingMethod = Literal["none", "one_hot", "ordinal", "binary_map"]
CriterionType = Literal["numeric", "categorical", "binary"]
DirectionType = Literal["maximize", "minimize", "target"]


class FieldConfig(BaseModel):
    key: str
    field_type: FieldType
    required: bool = False
    include_in_output: bool = True
    missing_strategy: MissingStrategy = "none"
    missing_constant: Any | None = None
    outlier_method: OutlierMethod = "none"
    outlier_threshold: float = 1.5
    normalization: NormalizationMethod = "none"
    encoding: EncodingMethod = "none"
    ordinal_map: dict[str, float] | None = None
    binary_map: dict[str, float] | None = None


class CriterionConfig(BaseModel):
    key: str
    name: str
    weight: float = Field(ge=0)
    type: CriterionType = "numeric"
    direction: DirectionType = "maximize"
    scale_map: dict[str, float] | None = None
    target_value: Any | None = None


class PipelineConfig(BaseModel):
    fields: list[FieldConfig]
    criteria: list[CriterionConfig]
    target_row_id: str | None = None
    analysis_mode: Literal["rating", "analog_search"] = "rating"
    top_n: int = 10
    project_id: int | None = None
    scenario_title: str | None = None
    parent_history_id: int | None = None


class PipelineRequest(BaseModel):
    filename: str
    config: PipelineConfig


class Contribution(BaseModel):
    key: str
    name: str
    raw_value: Any
    normalized_value: float
    weight: float
    contribution: float
    note: str | None = None


class RankedResult(BaseModel):
    object_id: str
    title: str
    rank: int
    score: float
    similarity_to_target: float | None = None
    explanation: str
    contributions: list[Contribution]


class PipelineRunResponse(BaseModel):
    import_preview: ImportedPreview
    preprocessing_summary: dict[str, Any]
    analysis_summary: dict[str, Any]
    ranking: list[RankedResult]


class ReportRequest(BaseModel):
    title: str = "Отчет сравнительного анализа"
    criteria: list[CriterionConfig]
    result: PipelineRunResponse
