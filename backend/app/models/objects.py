from sqlalchemy import Boolean, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.models.common import TimestampMixin


class ObjectType(Base, TimestampMixin):
    __tablename__ = "object_types"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    code: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    objects: Mapped[list["ObjectEntity"]] = relationship(back_populates="object_type")


class ObjectEntity(Base, TimestampMixin):
    __tablename__ = "objects"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    external_id: Mapped[str | None] = mapped_column(String(255), index=True)
    object_type_id: Mapped[int | None] = mapped_column(ForeignKey("object_types.id"))
    description: Mapped[str | None] = mapped_column(Text())
    source: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50), default="active")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    object_type: Mapped[ObjectType | None] = relationship(back_populates="objects")
    attributes: Mapped[list["ObjectAttribute"]] = relationship(
        back_populates="object",
        cascade="all, delete-orphan",
    )


class ObjectAttribute(Base, TimestampMixin):
    __tablename__ = "object_attributes"

    id: Mapped[int] = mapped_column(primary_key=True)
    object_id: Mapped[int] = mapped_column(ForeignKey("objects.id"), index=True)
    key: Mapped[str] = mapped_column(String(100), index=True)
    label: Mapped[str | None] = mapped_column(String(255))
    value_type: Mapped[str] = mapped_column(String(50), default="string")
    value_string: Mapped[str | None] = mapped_column(Text())
    value_number: Mapped[str | None] = mapped_column(String(100))
    value_boolean: Mapped[bool | None] = mapped_column(Boolean)
    value_json: Mapped[str | None] = mapped_column(Text())
    unit: Mapped[str | None] = mapped_column(String(50))
    is_filterable: Mapped[bool] = mapped_column(Boolean, default=True)
    is_analytic: Mapped[bool] = mapped_column(Boolean, default=True)

    object: Mapped[ObjectEntity] = relationship(back_populates="attributes")
