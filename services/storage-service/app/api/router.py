from fastapi import APIRouter

from app.api.files import router as files_router

api_router = APIRouter()
api_router.include_router(files_router)
