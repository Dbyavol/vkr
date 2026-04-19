from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.files import (
    ComparisonHistoryCreate,
    ComparisonHistoryRead,
    DatasetCreate,
    DatasetRead,
    FileRead,
    ProjectCreate,
    ProjectRead,
    StorageStats,
)
from app.services.file_service import (
    StorageAdapter,
    create_dataset,
    create_comparison_history,
    create_file_record,
    create_project,
    get_file,
    get_project,
    get_comparison_history,
    list_datasets,
    list_files,
    list_comparison_history,
    list_projects,
    storage_stats,
)

router = APIRouter(tags=["files"])


@router.get("/files", response_model=list[FileRead])
def read_files(db: Session = Depends(get_db)) -> list[FileRead]:
    return list_files(db)


@router.post("/files/upload", response_model=FileRead, status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    purpose: str = "upload",
    db: Session = Depends(get_db),
) -> FileRead:
    body = await file.read()
    adapter = StorageAdapter()
    key, checksum = adapter.upload(file.filename or "file.bin", file.content_type, body, prefix=purpose)
    return create_file_record(
        db,
        original_name=file.filename or "file.bin",
        content_type=file.content_type,
        purpose=purpose,
        storage_key=key,
        size_bytes=len(body),
        checksum=checksum,
    )


@router.get("/files/{file_id}", response_model=FileRead)
def read_file(file_id: int, db: Session = Depends(get_db)) -> FileRead:
    item = get_file(db, file_id)
    if item is None:
        raise HTTPException(status_code=404, detail="File not found")
    return item


@router.get("/files/{file_id}/download-url")
def get_download_url(file_id: int, db: Session = Depends(get_db)) -> dict[str, str]:
    item = get_file(db, file_id)
    if item is None:
        raise HTTPException(status_code=404, detail="File not found")
    adapter = StorageAdapter()
    return {"url": adapter.presigned_download_url(item.storage_key)}


@router.get("/datasets", response_model=list[DatasetRead])
def read_datasets(db: Session = Depends(get_db)) -> list[DatasetRead]:
    return list_datasets(db)


@router.post("/datasets", response_model=DatasetRead, status_code=201)
def create_dataset_endpoint(payload: DatasetCreate, db: Session = Depends(get_db)) -> DatasetRead:
    return create_dataset(db, payload)


@router.get("/projects", response_model=list[ProjectRead])
def read_projects(user_id: int | None = None, db: Session = Depends(get_db)) -> list[ProjectRead]:
    return list_projects(db, user_id=user_id)


@router.post("/projects", response_model=ProjectRead, status_code=201)
def create_project_endpoint(payload: ProjectCreate, db: Session = Depends(get_db)) -> ProjectRead:
    return create_project(db, payload)


@router.get("/projects/{project_id}", response_model=ProjectRead)
def read_project(project_id: int, db: Session = Depends(get_db)) -> ProjectRead:
    item = get_project(db, project_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return item


@router.get("/projects/{project_id}/history", response_model=list[ComparisonHistoryRead])
def read_project_history(project_id: int, db: Session = Depends(get_db)) -> list[ComparisonHistoryRead]:
    return list_comparison_history(db, project_id=project_id)


@router.get("/comparison-history", response_model=list[ComparisonHistoryRead])
def read_comparison_history(
    user_id: int | None = None,
    project_id: int | None = None,
    db: Session = Depends(get_db),
) -> list[ComparisonHistoryRead]:
    return list_comparison_history(db, user_id=user_id, project_id=project_id)


@router.get("/comparison-history/{history_id}", response_model=ComparisonHistoryRead)
def read_comparison_history_item(history_id: int, db: Session = Depends(get_db)) -> ComparisonHistoryRead:
    item = get_comparison_history(db, history_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Comparison history item not found")
    return item


@router.post("/comparison-history", response_model=ComparisonHistoryRead, status_code=201)
def create_comparison_history_endpoint(
    payload: ComparisonHistoryCreate,
    db: Session = Depends(get_db),
) -> ComparisonHistoryRead:
    return create_comparison_history(db, payload)


@router.get("/stats", response_model=StorageStats)
def read_storage_stats(user_id: int | None = None, db: Session = Depends(get_db)) -> StorageStats:
    return storage_stats(db, user_id=user_id)
