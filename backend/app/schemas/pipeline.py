from typing import Any, Literal

from pydantic import BaseModel, Field
from app.schemas.preprocessing import ProfileDetailLevel


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
    pre_normalized_preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str]
    normalized_dataset: dict[str, list[PipelineRow]]
    pre_normalized_dataset: dict[str, list[PipelineRow]] = Field(default_factory=dict)


FieldType = Literal["numeric", "integer", "float", "geo_latitude", "geo_longitude", "categorical", "binary", "text", "datetime"]
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
    use_in_label: bool = False
    missing_strategy: MissingStrategy = "none"
    missing_constant: Any | None = None
    outlier_method: OutlierMethod = "none"
    outlier_threshold: float = 1.5
    normalization: NormalizationMethod = "none"
    encoding: EncodingMethod = "none"
    rounding_precision: int | None = None
    datetime_format: str | None = None
    unit_family: str | None = None
    target_unit: str | None = None
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


class NumericRangeFilter(BaseModel):
    key: str
    min_value: float | None = None
    max_value: float | None = None


class CategoricalAllowlistFilter(BaseModel):
    key: str
    values: list[str] = Field(default_factory=list)


class AnalysisFilters(BaseModel):
    numeric_ranges: list[NumericRangeFilter] = Field(default_factory=list)
    categorical_allowlist: list[CategoricalAllowlistFilter] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    fields: list[FieldConfig]
    criteria: list[CriterionConfig]
    target_row_id: str | None = None
    geo_radius_km: float | None = Field(default=None, ge=0)
    analysis_mode: Literal["rating", "analog_search"] = "rating"
    top_n: int = 10
    enable_market_valuation: bool = False
    valuation_price_field_key: str | None = None
    valuation_analogs_count: int = Field(default=5, ge=1, le=100)
    filter_criteria: AnalysisFilters | None = None
    include_stability_scenarios: bool = False
    stability_variation_pct: float = Field(default=10.0, ge=0.0, le=100.0)
    project_id: int | None = None
    scenario_title: str | None = None
    parent_history_id: int | None = None


class PipelineRequest(BaseModel):
    filename: str
    config: PipelineConfig


class PipelineStoredRunRequest(BaseModel):
    filename: str | None = None
    dataset_file_id: int
    config: PipelineConfig


class PipelinePreprocessRefreshRequest(BaseModel):
    filename: str | None = None
    dataset_file_id: int
    fields: list[FieldConfig]
    histogram_bins: int = Field(default=8, ge=2, le=64)
    histogram_bins_by_field: dict[str, int] = Field(default_factory=dict)
    profile_detail_level: ProfileDetailLevel = "detailed"


class PipelinePreprocessRefreshResponse(BaseModel):
    preview: ImportedPreview
    profile: dict[str, Any]
    pre_normalized_profile: dict[str, Any] | None = None
    preprocessing_summary: dict[str, Any]


class PipelineProfileStoredResponse(BaseModel):
    dataset_file_id: int
    preview: ImportedPreview
    profile: dict[str, Any]
    pre_normalized_profile: dict[str, Any] | None = None


class PipelineStoredProfileRequest(BaseModel):
    filename: str | None = None
    dataset_file_id: int
    histogram_bins: int = Field(default=8, ge=2, le=64)
    histogram_bins_by_field: dict[str, int] = Field(default_factory=dict)
    profile_detail_level: ProfileDetailLevel = "detailed"


class PipelineRawObjectsRequest(BaseModel):
    filename: str | None = None
    dataset_file_id: int
    object_ids: list[str] = Field(default_factory=list)


class PipelineRawObjectsResponse(BaseModel):
    dataset_file_id: int
    objects: dict[str, dict[str, Any]]


class Contribution(BaseModel):
    key: str
    name: str
    raw_value: Any
    transformed_value: Any | None = None
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
    history_id: int | None = None


class ReportRequest(BaseModel):
    title: str = "Отчет сравнительного анализа"
    criteria: list[CriterionConfig]
    result: PipelineRunResponse
