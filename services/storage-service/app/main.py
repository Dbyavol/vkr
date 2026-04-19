from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text

from app.api.router import api_router
from app.core.config import get_settings
from app.db.base import Base
from app.db.session import engine
from app.models import files, objects  # noqa: F401

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    debug=settings.debug,
    description="Микросервис хранения универсальных объектов, атрибутов, датасетов и файлов в S3.",
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


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(api_router, prefix=settings.api_prefix)
