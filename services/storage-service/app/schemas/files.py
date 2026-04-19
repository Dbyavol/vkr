from datetime import datetime

from pydantic import BaseModel

from app.schemas.common import ORMModel


class FileRead(ORMModel):
    id: int
    original_name: str
    content_type: str | None = None
    purpose: str
    storage_key: str
    bucket_name: str
    size_bytes: int
    checksum: str | None = None
    metadata_json: str | None = None
    created_at: datetime


class DatasetCreate(BaseModel):
    name: str
    description: str | None = None
    object_type_id: int | None = None
    source_file_id: int | None = None
    row_count: int = 0
    status: str = "ready"
    schema_json: str | None = None


class DatasetRead(ORMModel):
    id: int
    name: str
    description: str | None = None
    object_type_id: int | None = None
    source_file_id: int | None = None
    row_count: int
    status: str
    schema_json: str | None = None
    created_at: datetime


class ProjectCreate(BaseModel):
    owner_user_id: int
    owner_email: str
    name: str
    description: str | None = None
    status: str = "active"
    metadata_json: str | None = None


class ProjectRead(ORMModel):
    id: int
    owner_user_id: int
    owner_email: str
    name: str
    description: str | None = None
    status: str
    metadata_json: str | None = None
    created_at: datetime


class ComparisonHistoryCreate(BaseModel):
    user_id: int
    user_email: str
    title: str = "Comparison"
    source_filename: str | None = None
    project_id: int | None = None
    version_number: int | None = None
    parent_history_id: int | None = None
    dataset_file_id: int | None = None
    result_file_id: int | None = None
    parameters_json: str
    summary_json: str
    tags_json: str | None = None
    status: str = "completed"


class ComparisonHistoryRead(ORMModel):
    id: int
    user_id: int
    user_email: str
    title: str
    source_filename: str | None = None
    project_id: int | None = None
    version_number: int
    parent_history_id: int | None = None
    dataset_file_id: int | None = None
    result_file_id: int | None = None
    parameters_json: str
    summary_json: str
    tags_json: str | None = None
    status: str
    created_at: datetime


class ComparisonHistoryResultFileUpdate(BaseModel):
    result_file_id: int


class StorageStats(BaseModel):
    files_total: int
    datasets_total: int
    comparisons_total: int
    projects_total: int
    storage_bytes_total: int
