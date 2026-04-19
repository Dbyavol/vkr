from fastapi import FastAPI, File, HTTPException, UploadFile

from app.schemas.imports import ImportCommitRequest, ImportCommitResponse, ImportParseRequest, ImportPreviewResponse
from app.services.import_parser import build_commit_response, parse_dataset_base64, parse_dataset_bytes

app = FastAPI(
    title="Import Service",
    version="0.1.0",
    description="Микросервис импорта файлов и подготовки унифицированного датасета для предобработки.",
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/imports/preview", response_model=ImportPreviewResponse)
async def preview_import(file: UploadFile = File(...)) -> ImportPreviewResponse:
    try:
        body = await file.read()
        return parse_dataset_bytes(file.filename or "dataset", body)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "IMPORT_VALIDATION_ERROR", "message": str(exc)},
        ) from exc


@app.post("/api/v1/imports/parse-base64", response_model=ImportPreviewResponse)
def parse_import_base64(payload: ImportParseRequest) -> ImportPreviewResponse:
    try:
        return parse_dataset_base64(payload.filename, payload.content_base64)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "IMPORT_VALIDATION_ERROR", "message": str(exc)},
        ) from exc


@app.post("/api/v1/imports/commit", response_model=ImportCommitResponse)
def commit_import(payload: ImportCommitRequest) -> ImportCommitResponse:
    try:
        return build_commit_response(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "IMPORT_COMMIT_ERROR", "message": str(exc)},
        ) from exc
