from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.logging import log_audit_event
from app.db.session import get_db
from app.schemas.objects import ObjectCreate, ObjectRead, ObjectTypeCreate, ObjectTypeRead, ObjectUpdate
from app.services.object_service import (
    create_object,
    create_object_type,
    delete_object,
    get_object,
    get_object_type,
    list_object_types,
    list_objects,
    update_object,
)

router = APIRouter(tags=["objects"])


@router.get("/object-types", response_model=list[ObjectTypeRead])
def read_object_types(db: Session = Depends(get_db)) -> list[ObjectTypeRead]:
    return list_object_types(db)


@router.post("/object-types", response_model=ObjectTypeRead, status_code=201)
def create_object_type_endpoint(payload: ObjectTypeCreate, db: Session = Depends(get_db)) -> ObjectTypeRead:
    item = create_object_type(db, payload)
    log_audit_event("object_type_created", object_type_id=item.id, name=item.name, code=item.code)
    return item


@router.get("/object-types/{object_type_id}", response_model=ObjectTypeRead)
def read_object_type(object_type_id: int, db: Session = Depends(get_db)) -> ObjectTypeRead:
    entity = get_object_type(db, object_type_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Object type not found")
    return entity


@router.get("/objects", response_model=list[ObjectRead])
def read_objects(
    active_only: bool = Query(default=True),
    db: Session = Depends(get_db),
) -> list[ObjectRead]:
    return list_objects(db, only_active=active_only)


@router.post("/objects", response_model=ObjectRead, status_code=201)
def create_object_endpoint(payload: ObjectCreate, db: Session = Depends(get_db)) -> ObjectRead:
    item = create_object(db, payload)
    log_audit_event("object_created", object_id=item.id, title=item.title, object_type_id=item.object_type_id)
    return item


@router.get("/objects/{object_id}", response_model=ObjectRead)
def read_object(object_id: int, db: Session = Depends(get_db)) -> ObjectRead:
    entity = get_object(db, object_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Object not found")
    return entity


@router.put("/objects/{object_id}", response_model=ObjectRead)
def update_object_endpoint(object_id: int, payload: ObjectUpdate, db: Session = Depends(get_db)) -> ObjectRead:
    entity = update_object(db, object_id, payload)
    if entity is None:
        raise HTTPException(status_code=404, detail="Object not found")
    log_audit_event("object_updated", object_id=entity.id, title=entity.title, object_type_id=entity.object_type_id)
    return entity


@router.delete("/objects/{object_id}", status_code=204)
def delete_object_endpoint(object_id: int, db: Session = Depends(get_db)) -> None:
    if not delete_object(db, object_id):
        raise HTTPException(status_code=404, detail="Object not found")
    log_audit_event("object_deleted", object_id=object_id)
