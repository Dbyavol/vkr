from app.schemas.files import ComparisonHistoryCreate, ProjectCreate
from app.schemas.objects import AttributeValue, ObjectCreate, ObjectTypeCreate
from app.services.file_service import (
    StorageAdapter,
    create_comparison_history,
    create_file_record,
    create_project,
    list_comparison_history,
    list_projects,
)
from app.services.object_service import create_object, create_object_type, get_object


def test_file_service_creates_project_and_history(db):
    project = create_project(
        db,
        ProjectCreate(owner_user_id=1, owner_email="admin@example.com", name="Demo Project"),
    )
    file_record = create_file_record(
        db,
        original_name="dataset.csv",
        content_type="text/csv",
        purpose="upload",
        storage_key="uploads/demo.csv",
        size_bytes=10,
        checksum="abc",
    )
    history = create_comparison_history(
        db,
        ComparisonHistoryCreate(
            user_id=1,
            user_email="admin@example.com",
            title="Run 1",
            source_filename="dataset.csv",
            project_id=project.id,
            dataset_file_id=file_record.id,
            result_file_id=file_record.id,
            parameters_json="{}",
            summary_json="{}",
        ),
    )

    assert list_projects(db)[0].id == project.id
    assert list_comparison_history(db, user_id=1)[0].id == history.id


def test_storage_adapter_local_mode_roundtrip():
    adapter = StorageAdapter()
    key, checksum = adapter.upload("sample.txt", "text/plain", b"hello", prefix="tests")
    body = adapter.download(key)

    assert body == b"hello"
    assert checksum


def test_object_service_creates_object_with_attributes(db):
    object_type = create_object_type(
        db,
        ObjectTypeCreate(name="Недвижимость", code="real_estate"),
    )
    entity = create_object(
        db,
        ObjectCreate(
            title="Квартира 1",
            object_type_id=object_type.id,
            attributes=[
                AttributeValue(key="price", value_type="number", value=10000000),
                AttributeValue(key="district", value_type="string", value="center"),
            ],
        ),
    )

    loaded = get_object(db, entity.id)
    assert loaded is not None
    assert loaded.title == "Квартира 1"
    assert len(loaded.attributes) == 2
