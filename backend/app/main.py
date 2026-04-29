from io import BytesIO

from docx import Document
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy import inspect, text

from app.api.auth import router as auth_router
from app.api.files import router as files_router
from app.api.objects import router as objects_router
from app.core.config import get_settings
from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.models import files, objects, user  # noqa: F401
from app.schemas.pipeline import (
    PipelineConfig,
    PipelinePreprocessRefreshRequest,
    PipelinePreprocessRefreshResponse,
    PipelineProfileStoredResponse,
    PipelineRequest,
    PipelineRunResponse,
    PipelineStoredRunRequest,
    ReportRequest,
)
from app.services.pipeline_engine import (
    fetch_system_dashboard,
    refresh_preprocessing_from_storage,
    run_pipeline_from_storage,
    run_pipeline_via_services,
    upload_and_profile_dataset,
)
from app.services.user_service import bootstrap_admin

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.2.0",
    debug=settings.debug,
    description="Модульный монолит для сравнительного анализа объектов.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)
    inspector = inspect(engine)
    if "comparison_history" in inspector.get_table_names():
        existing = {column["name"] for column in inspector.get_columns("comparison_history")}
        migrations = {
            "project_id": "ALTER TABLE comparison_history ADD COLUMN project_id INTEGER",
            "version_number": "ALTER TABLE comparison_history ADD COLUMN version_number INTEGER DEFAULT 1",
            "parent_history_id": "ALTER TABLE comparison_history ADD COLUMN parent_history_id INTEGER",
            "tags_json": "ALTER TABLE comparison_history ADD COLUMN tags_json TEXT",
        }
        with engine.begin() as connection:
            for column_name, ddl in migrations.items():
                if column_name not in existing:
                    connection.execute(text(ddl))

    db = SessionLocal()
    try:
        bootstrap_admin(db)
    finally:
        db.close()


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


SUMMARY_LABELS = {
    "objects_count": "Количество Объектов",
    "criteria_count": "Количество Критериев",
    "weights_sum": "Сумма Весов",
    "best_object_id": "Лучший Объект",
    "best_score": "Лучшая Оценка",
    "normalization_notes": "Нормализация",
    "mode": "Режим Анализа",
    "target_object_id": "Целевой Объект",
    "confidence_score": "Надежность Расчета",
    "confidence_notes": "Замечания К Качеству",
    "ranking_stability_note": "Устойчивость Рейтинга",
    "analog_groups": "Группы Аналогов",
    "dominance_pairs": "Доминирование Объектов",
}

DIRECTION_LABELS = {
    "maximize": "Максимизация",
    "minimize": "Минимизация",
    "target": "Близость К Целевому Значению",
}

MODE_LABELS = {
    "rating": "Рейтинг Объектов",
    "analog_search": "Поиск Аналогов",
}


def report_label(key: str) -> str:
    return SUMMARY_LABELS.get(key, key.replace("_", " ").title())


def report_value(key: str, value: object) -> str:
    if value is None:
        return "-"
    if key == "mode":
        return MODE_LABELS.get(str(value), str(value))
    if isinstance(value, list):
        if not value:
            return "Нет Данных"
        if all(isinstance(item, str) for item in value):
            return "; ".join(str(item) for item in value)
        return f"{len(value)} Записей"
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, dict):
        return "См. Детальный Раздел"
    return str(value)


@app.get(f"{settings.api_prefix}/system/dashboard")
async def system_dashboard(authorization: str | None = Header(default=None)) -> dict:
    return await fetch_system_dashboard(authorization=authorization)


@app.post(f"{settings.api_prefix}/pipeline/upload-profile", response_model=PipelineProfileStoredResponse)
async def pipeline_upload_profile(file: UploadFile = File(...)) -> PipelineProfileStoredResponse:
    try:
        body = await file.read()
        data = await upload_and_profile_dataset(filename=file.filename or "dataset", body=body)
        return PipelineProfileStoredResponse.model_validate(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"code": "PIPELINE_PROFILE_ERROR", "message": str(exc)}) from exc


@app.post(f"{settings.api_prefix}/pipeline/run", response_model=PipelineRunResponse)
async def pipeline_run(
    file: UploadFile = File(...),
    config_json: str = Form(...),
    authorization: str | None = Header(default=None),
) -> PipelineRunResponse:
    try:
        config = PipelineConfig.model_validate_json(config_json)
        body = await file.read()
        request = PipelineRequest(filename=file.filename or "dataset", config=config)
        return await run_pipeline_via_services(
            filename=file.filename or "dataset",
            body=body,
            payload=request,
            authorization=authorization,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"code": "PIPELINE_VALIDATION_ERROR", "message": str(exc)}) from exc


@app.post(f"{settings.api_prefix}/pipeline/run-stored", response_model=PipelineRunResponse)
async def pipeline_run_stored(
    payload: PipelineStoredRunRequest,
    authorization: str | None = Header(default=None),
) -> PipelineRunResponse:
    try:
        request = PipelineRequest(filename=payload.filename or "dataset", config=payload.config)
        return await run_pipeline_from_storage(
            dataset_file_id=payload.dataset_file_id,
            filename=payload.filename,
            payload=request,
            authorization=authorization,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"code": "PIPELINE_VALIDATION_ERROR", "message": str(exc)}) from exc


@app.post(f"{settings.api_prefix}/pipeline/preprocess-refresh", response_model=PipelinePreprocessRefreshResponse)
async def pipeline_preprocess_refresh(payload: PipelinePreprocessRefreshRequest) -> PipelinePreprocessRefreshResponse:
    try:
        data = await refresh_preprocessing_from_storage(
            dataset_file_id=payload.dataset_file_id,
            filename=payload.filename,
            fields=[field.model_dump() for field in payload.fields],
            histogram_bins=payload.histogram_bins,
            histogram_bins_by_field=payload.histogram_bins_by_field,
        )
        return PipelinePreprocessRefreshResponse.model_validate(data)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "PIPELINE_PREPROCESSING_REFRESH_ERROR", "message": str(exc)},
        ) from exc


@app.post(f"{settings.api_prefix}/reports/comparison.docx")
def comparison_report(payload: ReportRequest) -> StreamingResponse:
    document = Document()
    document.add_heading(payload.title, 0)
    document.add_paragraph("Отчет Сформирован Модульным Монолитом Системы Сравнительного Анализа.")

    document.add_heading("Сводка", level=1)
    summary_table = document.add_table(rows=1, cols=2)
    summary_table.style = "Table Grid"
    summary_table.rows[0].cells[0].text = "Показатель"
    summary_table.rows[0].cells[1].text = "Значение"
    for key, value in payload.result.analysis_summary.items():
        row = summary_table.add_row().cells
        row[0].text = report_label(str(key))
        row[1].text = report_value(str(key), value)

    document.add_heading("Критерии", level=1)
    criteria_table = document.add_table(rows=1, cols=4)
    criteria_table.style = "Table Grid"
    for cell, text_value in zip(criteria_table.rows[0].cells, ["Критерий", "Поле", "Вес", "Направление"]):
        cell.text = text_value
    for criterion in payload.criteria:
        row = criteria_table.add_row().cells
        row[0].text = criterion.name
        row[1].text = criterion.key
        row[2].text = f"{criterion.weight:.4f}"
        row[3].text = DIRECTION_LABELS.get(criterion.direction, criterion.direction)

    document.add_heading("Результаты", level=1)
    result_table = document.add_table(rows=1, cols=5)
    result_table.style = "Table Grid"
    for cell, text_value in zip(result_table.rows[0].cells, ["Место", "Объект", "Оценка", "Сходство", "Объяснение"]):
        cell.text = text_value
    for item in payload.result.ranking:
        row = result_table.add_row().cells
        row[0].text = str(item.rank)
        row[1].text = item.title
        row[2].text = f"{item.score:.4f}"
        row[3].text = "-" if item.similarity_to_target is None else f"{item.similarity_to_target:.4f}"
        row[4].text = item.explanation

    output = BytesIO()
    document.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": 'attachment; filename="comparison-report.docx"'},
    )


app.include_router(auth_router)
app.include_router(files_router, prefix=settings.api_prefix)
app.include_router(objects_router, prefix=settings.api_prefix)
