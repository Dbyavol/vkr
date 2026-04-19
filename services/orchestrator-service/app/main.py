from io import BytesIO

import httpx
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from docx import Document

from app.core_config import get_settings
from app.schemas.pipeline import ImportedPreview, PipelineConfig, PipelineRequest, PipelineRunResponse, ReportRequest
from app.services.pipeline_engine import (
    fetch_dataset_profile,
    fetch_preview,
    fetch_system_dashboard,
    run_pipeline_via_services,
)

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    debug=settings.debug,
    description="Единая точка входа, объединяющая импорт, предобработку и сравнительный анализ.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/system/dashboard")
async def system_dashboard(authorization: str | None = Header(default=None)) -> dict:
    return await fetch_system_dashboard(settings=settings, authorization=authorization)


@app.post("/api/v1/pipeline/preview", response_model=ImportedPreview)
async def pipeline_preview(file: UploadFile = File(...)) -> ImportedPreview:
    try:
        body = await file.read()
        data = await fetch_preview(settings=settings, filename=file.filename or "dataset", body=body)
        return ImportedPreview.model_validate(data)
    except (ValueError, httpx.HTTPError) as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "PIPELINE_PREVIEW_ERROR", "message": str(exc)},
        ) from exc


@app.post("/api/v1/pipeline/profile")
async def pipeline_profile(file: UploadFile = File(...)) -> dict:
    try:
        body = await file.read()
        return await fetch_dataset_profile(settings=settings, filename=file.filename or "dataset", body=body)
    except (ValueError, httpx.HTTPError) as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "PIPELINE_PROFILE_ERROR", "message": str(exc)},
        ) from exc


@app.post("/api/v1/pipeline/run", response_model=PipelineRunResponse)
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
            settings=settings,
            filename=file.filename or "dataset",
            body=body,
            payload=request,
            authorization=authorization,
        )
    except (ValueError, httpx.HTTPError) as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": "PIPELINE_VALIDATION_ERROR", "message": str(exc)},
        ) from exc


@app.post("/api/v1/reports/comparison.docx")
def comparison_report(payload: ReportRequest) -> StreamingResponse:
    document = Document()
    document.add_heading(payload.title, 0)
    document.add_paragraph("Сформировано информационно-аналитической системой сравнительного анализа объектов.")
    document.add_heading("Сводка", level=1)
    summary_table = document.add_table(rows=1, cols=2)
    summary_table.style = "Table Grid"
    summary_table.rows[0].cells[0].text = "Показатель"
    summary_table.rows[0].cells[1].text = "Значение"
    for key, value in payload.result.analysis_summary.items():
        row = summary_table.add_row().cells
        row[0].text = str(key)
        row[1].text = str(value)

    document.add_heading("Критерии", level=1)
    criteria_table = document.add_table(rows=1, cols=4)
    criteria_table.style = "Table Grid"
    for cell, text in zip(criteria_table.rows[0].cells, ["Критерий", "Поле", "Вес", "Направление"]):
        cell.text = text
    for criterion in payload.criteria:
        row = criteria_table.add_row().cells
        row[0].text = criterion.name
        row[1].text = criterion.key
        row[2].text = f"{criterion.weight:.4f}"
        row[3].text = criterion.direction

    document.add_heading("Результаты", level=1)
    result_table = document.add_table(rows=1, cols=5)
    result_table.style = "Table Grid"
    for cell, text in zip(result_table.rows[0].cells, ["Место", "Объект", "Оценка", "Сходство", "Объяснение"]):
        cell.text = text
    for item in payload.result.ranking:
        row = result_table.add_row().cells
        row[0].text = str(item.rank)
        row[1].text = item.title
        row[2].text = f"{item.score:.4f}"
        row[3].text = "-" if item.similarity_to_target is None else f"{item.similarity_to_target:.4f}"
        row[4].text = item.explanation

    document.add_heading("Вклад критериев для лучшего результата", level=1)
    if payload.result.ranking:
        contribution_table = document.add_table(rows=1, cols=5)
        contribution_table.style = "Table Grid"
        for cell, text in zip(contribution_table.rows[0].cells, ["Критерий", "Исходное значение", "Норм.", "Вес", "Вклад"]):
            cell.text = text
        for contribution in payload.result.ranking[0].contributions:
            row = contribution_table.add_row().cells
            row[0].text = contribution.name
            row[1].text = str(contribution.raw_value)
            row[2].text = f"{contribution.normalized_value:.4f}"
            row[3].text = f"{contribution.weight:.4f}"
            row[4].text = f"{contribution.contribution:.4f}"

    output = BytesIO()
    document.save(output)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": 'attachment; filename="comparison-report.docx"'},
    )
