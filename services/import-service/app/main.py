from fastapi import FastAPI, File, HTTPException, UploadFile

from app.schemas.imports import ImportParseRequest, ImportPreviewResponse
from app.services.import_parser import parse_dataset_base64

app = FastAPI(
    title="Import Service",
    version="0.1.0",
    description="Микросервис импорта файлов и подготовки унифицированного датасета для предобработки.",
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/imports/parse-base64", response_model=ImportPreviewResponse)
def parse_import_base64(payload: ImportParseRequest) -> ImportPreviewResponse:
    try:
        return parse_dataset_base64(payload.filename, payload.content_base64)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "IMPORT_VALIDATION_ERROR", "message": str(exc)},
        ) from exc


