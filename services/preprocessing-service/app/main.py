from fastapi import FastAPI, HTTPException

from app.schemas.preprocessing import DatasetProfileRequest, DatasetProfileResponse, PreprocessingRequest, PreprocessingResponse
from app.services.profiling_engine import profile_dataset
from app.services.preprocessing_engine import preprocess_dataset

app = FastAPI(
    title="Preprocessing Service",
    version="0.1.0",
    description="Микросервис предобработки входных датасетов для системы сравнительного анализа объектов.",
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/preprocessing/run", response_model=PreprocessingResponse)
def run_preprocessing(payload: PreprocessingRequest) -> PreprocessingResponse:
    try:
        return preprocess_dataset(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "PREPROCESSING_VALIDATION_ERROR",
                "message": str(exc),
            },
        ) from exc


@app.post("/api/v1/preprocessing/profile", response_model=DatasetProfileResponse)
def run_profile(payload: DatasetProfileRequest) -> DatasetProfileResponse:
    try:
        return profile_dataset(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "PROFILE_VALIDATION_ERROR",
                "message": str(exc),
            },
        ) from exc
