from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent
BACKEND_DIR = TEST_DIR.parent
DB_PATH = TEST_DIR / "test_app.db"
STORAGE_DIR = Path(tempfile.mkdtemp(prefix="backend-test-storage-"))

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ["BACKEND_DATABASE_URL"] = f"sqlite:///{DB_PATH.as_posix()}"
os.environ["BACKEND_S3_ENDPOINT_URL"] = ""
os.environ["BACKEND_LOCAL_STORAGE_DIR"] = str(STORAGE_DIR)
os.environ["BACKEND_BOOTSTRAP_ADMIN_EMAIL"] = "admin@example.com"
os.environ["BACKEND_BOOTSTRAP_ADMIN_PASSWORD"] = "admin12345"
os.environ["BACKEND_JWT_SECRET"] = "test-secret"

import pytest
from fastapi.testclient import TestClient

from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.main import app
from app.services.user_service import bootstrap_admin


def _reset_storage_dir() -> None:
    if STORAGE_DIR.exists():
        shutil.rmtree(STORAGE_DIR)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)


@pytest.fixture(autouse=True)
def reset_database() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    _reset_storage_dir()
    db = SessionLocal()
    try:
        bootstrap_admin(db)
    finally:
        db.close()


@pytest.fixture
def db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def sample_csv_bytes() -> bytes:
    return (
        "price,area,rooms,condition,district\n"
        "100,50,2,new,center\n"
        "120,55,2,good,north\n"
        "90,48,1,old,center\n"
    ).encode("utf-8")


@pytest.fixture(scope="session", autouse=True)
def cleanup_session_artifacts():
    yield
    engine.dispose()
    if DB_PATH.exists():
        DB_PATH.unlink()
    if STORAGE_DIR.exists():
        shutil.rmtree(STORAGE_DIR, ignore_errors=True)
