from typing import Any

from pydantic import BaseModel, Field, model_validator


class ImportedRow(BaseModel):
    id: str
    values: dict[str, Any]


class ImportedDataset(BaseModel):
    rows: list[ImportedRow]


class ColumnInfo(BaseModel):
    source_name: str
    normalized_name: str
    inferred_type: str
    missing_count: int
    unique_count: int
    sample_values: list[Any]


class ImportPreviewResponse(BaseModel):
    filename: str
    rows_total: int
    columns: list[ColumnInfo]
    preview_rows: list[dict[str, Any]]
    warnings: list[str]
    normalized_dataset: ImportedDataset


class ImportCommitRequest(BaseModel):
    dataset_name: str
    object_type_code: str | None = None
    source_filename: str
    dataset: ImportedDataset
    schema_hint: dict[str, Any] | None = None
    persist_to_storage: bool = False
    storage_file_id: int | None = None

    @model_validator(mode="after")
    def validate_storage_link(self) -> "ImportCommitRequest":
        if self.persist_to_storage and self.storage_file_id is None:
            raise ValueError("storage_file_id is required when persist_to_storage=true")
        return self


class ImportCommitResponse(BaseModel):
    dataset_name: str
    rows_total: int
    status: str
    handoff_payload: dict[str, Any]


class ImportParseRequest(BaseModel):
    filename: str
    content_base64: str = Field(min_length=1)
