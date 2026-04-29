import hashlib
import time
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.files import ComparisonHistory, Dataset, Project, StoredFile
from app.schemas.files import ComparisonHistoryCreate, DatasetCreate, ProjectCreate, StorageStats

settings = get_settings()


class StorageAdapter:
    def __init__(self) -> None:
        self.bucket_name = settings.s3_bucket_name
        self.local_mode = not bool(settings.s3_endpoint_url)
        self.base_dir = Path(settings.local_storage_dir)
        self.client = None
        if self.local_mode:
            self.base_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.client = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint_url,
                region_name=settings.s3_region,
                aws_access_key_id=settings.s3_access_key_id,
                aws_secret_access_key=settings.s3_secret_access_key,
            )
            self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        assert self.client is not None
        last_error: Exception | None = None
        for _ in range(12):
            try:
                self.client.head_bucket(Bucket=self.bucket_name)
                return
            except ClientError as exc:
                last_error = exc
                status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status_code == 404:
                    self.client.create_bucket(Bucket=self.bucket_name)
                    return
                time.sleep(2)
            except BotoCoreError as exc:
                last_error = exc
                time.sleep(2)
        if last_error is not None:
            raise last_error

    def upload(self, filename: str, content_type: str | None, body: bytes, prefix: str = "uploads") -> tuple[str, str]:
        checksum = hashlib.sha256(body).hexdigest()
        key = f"{prefix}/{uuid4()}-{filename}"
        self.put(key, body, content_type)
        return key, checksum

    def put(self, key: str, body: bytes, content_type: str | None = None) -> None:
        if self.local_mode:
            path = self.base_dir / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        else:
            extra_args = {"ContentType": content_type} if content_type else None
            assert self.client is not None
            self.client.put_object(Bucket=self.bucket_name, Key=key, Body=body, **(extra_args or {}))

    def download(self, key: str) -> bytes:
        if self.local_mode:
            return (self.base_dir / key).read_bytes()
        assert self.client is not None
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        if self.local_mode:
            return (self.base_dir / key).exists()
        assert self.client is not None
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def presigned_download_url(self, key: str, expires_in: int = 3600) -> str:
        if self.local_mode:
            return str((self.base_dir / key).resolve())
        assert self.client is not None
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket_name, "Key": key},
            ExpiresIn=expires_in,
        )


def create_file_record(
    db: Session,
    *,
    original_name: str,
    content_type: str | None,
    purpose: str,
    storage_key: str,
    size_bytes: int,
    checksum: str | None,
    metadata_json: str | None = None,
) -> StoredFile:
    model = StoredFile(
        original_name=original_name,
        content_type=content_type,
        purpose=purpose,
        storage_key=storage_key,
        bucket_name=settings.s3_bucket_name,
        size_bytes=size_bytes,
        checksum=checksum,
        metadata_json=metadata_json,
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


def list_files(db: Session) -> list[StoredFile]:
    return list(db.scalars(select(StoredFile).order_by(StoredFile.id.desc())))


def get_file(db: Session, file_id: int) -> StoredFile | None:
    return db.get(StoredFile, file_id)


def create_dataset(db: Session, payload: DatasetCreate) -> Dataset:
    model = Dataset(
        name=payload.name,
        description=payload.description,
        object_type_id=payload.object_type_id,
        source_file_id=payload.source_file_id,
        row_count=payload.row_count,
        status=payload.status,
        schema_json=payload.schema_json,
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


def list_datasets(db: Session) -> list[Dataset]:
    return list(db.scalars(select(Dataset).order_by(Dataset.id.desc())))


def create_project(db: Session, payload: ProjectCreate) -> Project:
    model = Project(
        owner_user_id=payload.owner_user_id,
        owner_email=payload.owner_email,
        name=payload.name,
        description=payload.description,
        status=payload.status,
        metadata_json=payload.metadata_json,
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


def list_projects(db: Session, user_id: int | None = None) -> list[Project]:
    stmt = select(Project).order_by(Project.id.desc())
    if user_id is not None:
        stmt = stmt.where(Project.owner_user_id == user_id)
    return list(db.scalars(stmt))


def get_project(db: Session, project_id: int) -> Project | None:
    return db.get(Project, project_id)


def create_comparison_history(db: Session, payload: ComparisonHistoryCreate) -> ComparisonHistory:
    version_number = payload.version_number
    if version_number is None:
        version_number = 1
        if payload.project_id is not None:
            max_version = db.scalar(
                select(func.max(ComparisonHistory.version_number)).where(
                    ComparisonHistory.project_id == payload.project_id
                )
            )
            version_number = int(max_version or 0) + 1
    model = ComparisonHistory(
        user_id=payload.user_id,
        user_email=payload.user_email,
        title=payload.title,
        source_filename=payload.source_filename,
        project_id=payload.project_id,
        version_number=version_number,
        parent_history_id=payload.parent_history_id,
        dataset_file_id=payload.dataset_file_id,
        result_file_id=payload.result_file_id,
        parameters_json=payload.parameters_json,
        summary_json=payload.summary_json,
        tags_json=payload.tags_json,
        status=payload.status,
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


def get_comparison_history(db: Session, history_id: int) -> ComparisonHistory | None:
    return db.get(ComparisonHistory, history_id)


def update_comparison_history_result_file(
    db: Session,
    *,
    history_id: int,
    result_file_id: int,
) -> ComparisonHistory | None:
    item = db.get(ComparisonHistory, history_id)
    if item is None:
        return None
    item.result_file_id = result_file_id
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def list_comparison_history(
    db: Session,
    user_id: int | None = None,
    project_id: int | None = None,
) -> list[ComparisonHistory]:
    stmt = select(ComparisonHistory).order_by(ComparisonHistory.id.desc())
    if user_id is not None:
        stmt = stmt.where(ComparisonHistory.user_id == user_id)
    if project_id is not None:
        stmt = stmt.where(ComparisonHistory.project_id == project_id)
    return list(db.scalars(stmt))


def storage_stats(db: Session, user_id: int | None = None) -> StorageStats:
    history_stmt = select(func.count()).select_from(ComparisonHistory)
    project_stmt = select(func.count()).select_from(Project)
    if user_id is not None:
        history_stmt = history_stmt.where(ComparisonHistory.user_id == user_id)
        project_stmt = project_stmt.where(Project.owner_user_id == user_id)
    return StorageStats(
        files_total=int(db.scalar(select(func.count()).select_from(StoredFile)) or 0),
        datasets_total=int(db.scalar(select(func.count()).select_from(Dataset)) or 0),
        comparisons_total=int(db.scalar(history_stmt) or 0),
        projects_total=int(db.scalar(project_stmt) or 0),
        storage_bytes_total=int(db.scalar(select(func.coalesce(func.sum(StoredFile.size_bytes), 0))) or 0),
    )
