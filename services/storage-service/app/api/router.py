from fastapi import APIRouter

from app.api.files import router as files_router
from app.api.objects import router as objects_router

api_router = APIRouter()
api_router.include_router(objects_router)
api_router.include_router(files_router)
