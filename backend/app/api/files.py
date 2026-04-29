from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.core.logging import log_audit_event
from app.db.session import get_db
from app.schemas.files import (
    ComparisonHistoryCreate,
    ComparisonHistoryRead,
    ComparisonHistoryResultFileUpdate,
    FileRead,
    ProjectCreate,
    ProjectRead,
    StorageStats,
)
from app.services.file_service import (
    StorageAdapter,
    create_comparison_history,
    create_file_record,
    create_project,
    get_file,
    list_comparison_history,
    list_files,
    list_projects,
    storage_stats,
    update_comparison_history_result_file,
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
    item = create_file_record(
        db,
        original_name=file.filename or "file.bin",
        content_type=file.content_type,
        purpose=purpose,
        storage_key=key,
        size_bytes=len(body),
        checksum=checksum,
    )
    log_audit_event(
        "file_uploaded",
        file_id=item.id,
        filename=item.original_name,
        purpose=purpose,
        size_bytes=len(body),
    )
    return item


@router.get("/files/{file_id}", response_model=FileRead)
def read_file(file_id: int, db: Session = Depends(get_db)) -> FileRead:
    item = get_file(db, file_id)
    if item is None:
        raise HTTPException(status_code=404, detail="File not found")
    return item


@router.get("/files/{file_id}/content")
def download_file_content(file_id: int, db: Session = Depends(get_db)) -> Response:
    item = get_file(db, file_id)
    if item is None:
        raise HTTPException(status_code=404, detail="File not found")
    adapter = StorageAdapter()
    body = adapter.download(item.storage_key)
    log_audit_event("file_downloaded", file_id=item.id, filename=item.original_name, size_bytes=item.size_bytes)
    return Response(
        content=body,
        media_type=item.content_type or "application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{item.original_name}"'},
    )


@router.get("/projects", response_model=list[ProjectRead])
def read_projects(user_id: int | None = None, db: Session = Depends(get_db)) -> list[ProjectRead]:
    return list_projects(db, user_id=user_id)


@router.post("/projects", response_model=ProjectRead, status_code=201)
def create_project_endpoint(payload: ProjectCreate, db: Session = Depends(get_db)) -> ProjectRead:
    project = create_project(db, payload)
    log_audit_event(
        "project_created",
        project_id=project.id,
        owner_user_id=project.owner_user_id,
        owner_email=project.owner_email,
        name=project.name,
    )
    return project


@router.get("/comparison-history", response_model=list[ComparisonHistoryRead])
def read_comparison_history(
    user_id: int | None = None,
    project_id: int | None = None,
    db: Session = Depends(get_db),
) -> list[ComparisonHistoryRead]:
    return list_comparison_history(db, user_id=user_id, project_id=project_id)


@router.patch("/comparison-history/{history_id}/result-file", response_model=ComparisonHistoryRead)
def update_comparison_history_result_file_endpoint(
    history_id: int,
    payload: ComparisonHistoryResultFileUpdate,
    db: Session = Depends(get_db),
) -> ComparisonHistoryRead:
    item = update_comparison_history_result_file(
        db,
        history_id=history_id,
        result_file_id=payload.result_file_id,
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Comparison history item not found")
    log_audit_event(
        "comparison_history_result_file_updated",
        history_id=history_id,
        result_file_id=payload.result_file_id,
    )
    return item


@router.post("/comparison-history", response_model=ComparisonHistoryRead, status_code=201)
def create_comparison_history_endpoint(
    payload: ComparisonHistoryCreate,
    db: Session = Depends(get_db),
) -> ComparisonHistoryRead:
    item = create_comparison_history(db, payload)
    log_audit_event(
        "comparison_history_created",
        history_id=item.id,
        user_id=item.user_id,
        project_id=item.project_id,
        title=item.title,
    )
    return item


@router.get("/stats", response_model=StorageStats)
def read_storage_stats(user_id: int | None = None, db: Session = Depends(get_db)) -> StorageStats:
    return storage_stats(db, user_id=user_id)
