from app.schemas.objects import ObjectCreate


def test_object_create_payload_accepts_universal_attributes() -> None:
    payload = ObjectCreate.model_validate(
        {
            "title": "Universal object",
            "external_id": "obj-1",
            "status": "active",
            "attributes": [
                {"key": "price", "value_type": "number", "value": 123.45},
                {"key": "category", "value_type": "string", "value": "alpha"},
                {"key": "flag", "value_type": "boolean", "value": True},
            ],
        }
    )

    assert payload.title == "Universal object"
    assert len(payload.attributes) == 3
    assert payload.attributes[0].value_type == "number"
