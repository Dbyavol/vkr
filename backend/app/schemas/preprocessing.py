from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


FieldType = Literal["numeric", "integer", "float", "categorical", "binary", "text", "datetime"]
MissingStrategy = Literal["none", "drop_row", "mean", "median", "mode", "constant"]
OutlierMethod = Literal["none", "iqr_remove", "iqr_clip", "zscore_remove", "zscore_clip"]
NormalizationMethod = Literal["none", "minmax", "zscore", "robust", "log_minmax"]
EncodingMethod = Literal["none", "one_hot", "ordinal", "binary_map"]
ProfileDetailLevel = Literal["summary", "detailed"]


class DatasetRow(BaseModel):
    id: str
    values: dict[str, Any]


class DatasetPayload(BaseModel):
    rows: list[DatasetRow]


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
    rounding_precision: int | None = None
    datetime_format: str | None = None
    ordinal_map: dict[str, float] | None = None
    binary_map: dict[str, float] | None = None

    @model_validator(mode="after")
    def validate_field_rules(self) -> "FieldConfig":
        if self.missing_strategy == "constant" and self.missing_constant is None:
            raise ValueError("missing_constant is required when missing_strategy=constant")
        if self.encoding == "ordinal" and not self.ordinal_map:
            raise ValueError("ordinal_map is required when encoding=ordinal")
        if self.encoding == "binary_map" and not self.binary_map:
            raise ValueError("binary_map is required when encoding=binary_map")
        if self.rounding_precision is not None and self.rounding_precision < 0:
            raise ValueError("rounding_precision must be >= 0")
        return self


class PreprocessingOptions(BaseModel):
    drop_duplicate_rows: bool = True
    duplicate_keys: list[str] = Field(default_factory=list)
    keep_columns_not_in_config: bool = False
    preserve_original_values: bool = False


class PreprocessingRequest(BaseModel):
    dataset: DatasetPayload
    fields: list[FieldConfig]
    options: PreprocessingOptions = Field(default_factory=PreprocessingOptions)

    @model_validator(mode="after")
    def validate_payload(self) -> "PreprocessingRequest":
        if not self.dataset.rows:
            raise ValueError("dataset.rows must contain at least one row")
        if not self.fields:
            raise ValueError("fields must contain at least one field config")
        return self


class FieldReport(BaseModel):
    key: str
    field_type: str
    rows_missing_before: int
    rows_missing_after: int
    rows_removed_as_outliers: int
    values_clipped: int
    encoding_applied: str
    normalization_applied: str
    notes: list[str]


class ProcessedRow(BaseModel):
    id: str
    values: dict[str, Any]
    original_values: dict[str, Any] | None = None


class PreprocessingSummary(BaseModel):
    rows_input: int
    rows_output: int
    rows_removed_duplicates: int
    rows_removed_missing: int
    rows_removed_outliers: int
    generated_columns: list[str]


class PreprocessingResponse(BaseModel):
    summary: PreprocessingSummary
    field_reports: list[FieldReport]
    dataset: list[ProcessedRow]


class ChartPoint(BaseModel):
    label: str
    value: float


class FieldRecommendation(BaseModel):
    code: str
    severity: str
    message: str
    suggested_patch: dict[str, Any]


class FieldProfile(BaseModel):
    key: str
    inferred_type: FieldType
    analytic_candidate: bool
    rows_total: int
    missing_count: int
    unique_count: int
    unique_ratio: float
    sample_values: list[Any]
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_mean: float | None = None
    numeric_median: float | None = None
    outlier_count_iqr: int = 0
    histogram: list[ChartPoint] = Field(default_factory=list)
    top_categories: list[ChartPoint] = Field(default_factory=list)
    text_to_categorical_possible: bool = False
    recommended_config: FieldConfig
    recommendations: list[FieldRecommendation]


class DatasetQualityIssue(BaseModel):
    code: str
    severity: str
    message: str
    affected_fields: list[str] = Field(default_factory=list)
    penalty: float = 0.0


class DatasetQualityReport(BaseModel):
    score: float
    level: str
    readiness_label: str
    analytic_fields_count: int
    numeric_fields_count: int
    categorical_fields_count: int
    text_fields_count: int
    total_missing_values: int
    total_outliers_iqr: int
    issues: list[DatasetQualityIssue] = Field(default_factory=list)


class DatasetProfileRequest(BaseModel):
    dataset: DatasetPayload
    max_unique_values: int = 30
    histogram_bins: int = 8
    histogram_bins_by_field: dict[str, int] = Field(default_factory=dict)
    detail_level: ProfileDetailLevel = "detailed"


class DatasetProfileResponse(BaseModel):
    rows_total: int
    detail_level: ProfileDetailLevel = "detailed"
    fields: list[FieldProfile]
    quality: DatasetQualityReport
    recommended_weights: dict[str, float] = Field(default_factory=dict)
    weight_notes: list[str] = Field(default_factory=list)
    missing_matrix_preview: list[dict[str, Any]] = Field(default_factory=list)
    correlation_matrix: list[dict[str, Any]] = Field(default_factory=list)
