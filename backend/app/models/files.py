from sqlalchemy import BigInteger, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.models.common import TimestampMixin


class StoredFile(Base, TimestampMixin):
    __tablename__ = "stored_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    original_name: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str | None] = mapped_column(String(100))
    purpose: Mapped[str] = mapped_column(String(50), default="upload")
    storage_key: Mapped[str] = mapped_column(String(500), unique=True, index=True)
    bucket_name: Mapped[str] = mapped_column(String(100))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    checksum: Mapped[str | None] = mapped_column(String(128))
    metadata_json: Mapped[str | None] = mapped_column(Text())

    datasets: Mapped[list["Dataset"]] = relationship(back_populates="source_file")


class Dataset(Base, TimestampMixin):
    __tablename__ = "datasets"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str | None] = mapped_column(Text())
    object_type_id: Mapped[int | None] = mapped_column(ForeignKey("object_types.id"))
    source_file_id: Mapped[int | None] = mapped_column(ForeignKey("stored_files.id"))
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(50), default="ready")
    schema_json: Mapped[str | None] = mapped_column(Text())

    source_file: Mapped[StoredFile | None] = relationship(back_populates="datasets")


class Project(Base, TimestampMixin):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(primary_key=True)
    owner_user_id: Mapped[int] = mapped_column(Integer, index=True)
    owner_email: Mapped[str] = mapped_column(String(255), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str | None] = mapped_column(Text())
    status: Mapped[str] = mapped_column(String(50), default="active")
    metadata_json: Mapped[str | None] = mapped_column(Text())

    histories: Mapped[list["ComparisonHistory"]] = relationship(back_populates="project")


class ComparisonHistory(Base, TimestampMixin):
    __tablename__ = "comparison_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    user_email: Mapped[str] = mapped_column(String(255), index=True)
    title: Mapped[str] = mapped_column(String(255), default="Comparison")
    source_filename: Mapped[str | None] = mapped_column(String(255))
    project_id: Mapped[int | None] = mapped_column(ForeignKey("projects.id"), index=True)
    version_number: Mapped[int] = mapped_column(Integer, default=1)
    parent_history_id: Mapped[int | None] = mapped_column(ForeignKey("comparison_history.id"))
    dataset_file_id: Mapped[int | None] = mapped_column(ForeignKey("stored_files.id"))
    result_file_id: Mapped[int | None] = mapped_column(ForeignKey("stored_files.id"))
    parameters_json: Mapped[str] = mapped_column(Text())
    summary_json: Mapped[str] = mapped_column(Text())
    tags_json: Mapped[str | None] = mapped_column(Text())
    status: Mapped[str] = mapped_column(String(50), default="completed")

    project: Mapped[Project | None] = relationship(back_populates="histories")
