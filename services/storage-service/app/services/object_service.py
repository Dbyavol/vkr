import json
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.orm import Session, selectinload

from app.models.objects import ObjectAttribute, ObjectEntity, ObjectType
from app.schemas.objects import AttributeValue, ObjectCreate, ObjectTypeCreate, ObjectUpdate


def _assign_attribute(model: ObjectAttribute, attribute: AttributeValue) -> None:
    model.key = attribute.key
    model.label = attribute.label
    model.value_type = attribute.value_type
    model.unit = attribute.unit
    model.is_filterable = attribute.is_filterable
    model.is_analytic = attribute.is_analytic
    model.value_string = None
    model.value_number = None
    model.value_boolean = None
    model.value_json = None

    value: Any = attribute.value
    if attribute.value_type == "number":
        model.value_number = None if value is None else str(value)
    elif attribute.value_type == "boolean":
        model.value_boolean = None if value is None else bool(value)
    elif attribute.value_type == "json":
        model.value_json = None if value is None else json.dumps(value, ensure_ascii=False)
    else:
        model.value_string = None if value is None else str(value)


def create_object_type(db: Session, payload: ObjectTypeCreate) -> ObjectType:
    model = ObjectType(
        name=payload.name,
        code=payload.code,
        description=payload.description,
        is_active=payload.is_active,
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


def list_object_types(db: Session) -> list[ObjectType]:
    return list(db.scalars(select(ObjectType).order_by(ObjectType.name.asc())))


def get_object_type(db: Session, object_type_id: int) -> ObjectType | None:
    return db.get(ObjectType, object_type_id)


def _base_query() -> Select[tuple[ObjectEntity]]:
    return select(ObjectEntity).options(selectinload(ObjectEntity.attributes))


def list_objects(db: Session, only_active: bool = True) -> list[ObjectEntity]:
    stmt = _base_query().order_by(ObjectEntity.id.desc())
    if only_active:
        stmt = stmt.where(ObjectEntity.is_active.is_(True))
    return list(db.scalars(stmt))


def get_object(db: Session, object_id: int) -> ObjectEntity | None:
    return db.scalar(_base_query().where(ObjectEntity.id == object_id))


def create_object(db: Session, payload: ObjectCreate) -> ObjectEntity:
    entity = ObjectEntity(
        title=payload.title,
        external_id=payload.external_id,
        object_type_id=payload.object_type_id,
        description=payload.description,
        source=payload.source,
        status=payload.status,
        is_active=payload.is_active,
    )
    for attribute in payload.attributes:
        item = ObjectAttribute()
        _assign_attribute(item, attribute)
        entity.attributes.append(item)
    db.add(entity)
    db.commit()
    db.refresh(entity)
    return get_object(db, entity.id)  # type: ignore[return-value]


def update_object(db: Session, object_id: int, payload: ObjectUpdate) -> ObjectEntity | None:
    entity = get_object(db, object_id)
    if entity is None:
        return None
    entity.title = payload.title
    entity.external_id = payload.external_id
    entity.object_type_id = payload.object_type_id
    entity.description = payload.description
    entity.source = payload.source
    entity.status = payload.status
    entity.is_active = payload.is_active
    entity.attributes.clear()
    for attribute in payload.attributes:
        item = ObjectAttribute()
        _assign_attribute(item, attribute)
        entity.attributes.append(item)
    db.commit()
    db.refresh(entity)
    return get_object(db, entity.id)


def delete_object(db: Session, object_id: int) -> bool:
    entity = db.get(ObjectEntity, object_id)
    if entity is None:
        return False
    db.delete(entity)
    db.commit()
    return True
