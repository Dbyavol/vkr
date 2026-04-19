from fastapi import FastAPI, HTTPException

from app.schemas.analysis import AnalysisRequest, AnalysisResponse
from app.services.analysis_engine import run_comparative_analysis

app = FastAPI(
    title="Comparative Analysis Service",
    version="0.1.0",
    description="Микросервис сравнительного анализа объектов на основе метода взвешенных коэффициентов.",
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/analysis/run", response_model=AnalysisResponse)
def run_analysis(payload: AnalysisRequest) -> AnalysisResponse:
    try:
        return run_comparative_analysis(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "VALIDATION_ERROR",
                "message": str(exc),
            },
        ) from exc
