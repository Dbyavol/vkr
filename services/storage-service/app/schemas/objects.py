from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.schemas.common import ORMModel


class AttributeValue(BaseModel):
    key: str
    label: str | None = None
    value_type: str = "string"
    value: Any | None = None
    unit: str | None = None
    is_filterable: bool = True
    is_analytic: bool = True


class ObjectTypeCreate(BaseModel):
    name: str
    code: str
    description: str | None = None
    is_active: bool = True


class ObjectTypeRead(ORMModel):
    id: int
    name: str
    code: str
    description: str | None = None
    is_active: bool


class ObjectCreate(BaseModel):
    title: str
    external_id: str | None = None
    object_type_id: int | None = None
    description: str | None = None
    source: str | None = None
    status: str = "active"
    is_active: bool = True
    attributes: list[AttributeValue] = Field(default_factory=list)


class ObjectUpdate(ObjectCreate):
    pass


class ObjectAttributeRead(ORMModel):
    id: int
    key: str
    label: str | None = None
    value_type: str
    value_string: str | None = None
    value_number: str | None = None
    value_boolean: bool | None = None
    value_json: str | None = None
    unit: str | None = None
    is_filterable: bool
    is_analytic: bool


class ObjectRead(ORMModel):
    id: int
    title: str
    external_id: str | None = None
    object_type_id: int | None = None
    description: str | None = None
    source: str | None = None
    status: str
    is_active: bool
    created_at: datetime
    updated_at: datetime
    attributes: list[ObjectAttributeRead]
