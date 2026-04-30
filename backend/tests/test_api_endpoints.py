import json


def test_auth_endpoints_register_login_and_me(client):
    register_response = client.post(
        "/api/v1/auth/register",
        json={"email": "user@example.com", "full_name": "Test User", "password": "secret123"},
    )
    assert register_response.status_code == 201
    token = register_response.json()["access_token"]

    login_response = client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "secret123"},
    )
    assert login_response.status_code == 200

    me_response = client.get("/api/v1/users/me", headers={"Authorization": f"Bearer {token}"})
    assert me_response.status_code == 200
    assert me_response.json()["email"] == "user@example.com"


def test_file_project_and_object_endpoints(client):
    upload_response = client.post(
        "/api/v1/files/upload?purpose=test-upload",
        files={"file": ("dataset.csv", b"price,area\n100,50\n", "text/csv")},
    )
    assert upload_response.status_code == 201
    file_id = upload_response.json()["id"]

    project_response = client.post(
        "/api/v1/projects",
        json={"owner_user_id": 1, "owner_email": "admin@example.com", "name": "Project 1"},
    )
    assert project_response.status_code == 201

    object_type_response = client.post(
        "/api/v1/object-types",
        json={"name": "Товар", "code": "product"},
    )
    assert object_type_response.status_code == 201
    object_type_id = object_type_response.json()["id"]

    object_response = client.post(
        "/api/v1/objects",
        json={
            "title": "Объект 1",
            "object_type_id": object_type_id,
            "attributes": [{"key": "price", "value_type": "number", "value": 100}],
        },
    )
    assert object_response.status_code == 201
    assert file_id > 0


def test_pipeline_endpoints_and_dashboard(client, sample_csv_bytes: bytes):
    profile_response = client.post(
        "/api/v1/pipeline/upload-profile",
        files={"file": ("dataset.csv", sample_csv_bytes, "text/csv")},
    )
    assert profile_response.status_code == 200
    dataset_file_id = profile_response.json()["dataset_file_id"]

    config = {
        "fields": [
            {"key": "price", "field_type": "numeric", "normalization": "minmax"},
            {"key": "area", "field_type": "numeric", "normalization": "minmax"},
            {"key": "rooms", "field_type": "numeric", "normalization": "minmax"},
        ],
        "criteria": [
            {"key": "price", "name": "Цена", "weight": 0.6, "type": "numeric", "direction": "minimize"},
            {"key": "area", "name": "Площадь", "weight": 0.4, "type": "numeric", "direction": "maximize"},
        ],
    }
    run_response = client.post(
        "/api/v1/pipeline/run-stored",
        json={"filename": "dataset.csv", "dataset_file_id": dataset_file_id, "config": config},
    )
    assert run_response.status_code == 200
    ranking = run_response.json()["ranking"]
    assert len(ranking) >= 1

    login_response = client.post(
        "/api/v1/auth/login",
        json={"email": "admin@example.com", "password": "admin12345"},
    )
    token = login_response.json()["access_token"]
    dashboard_response = client.get(
        "/api/v1/system/dashboard",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["services"]["backend"]["status"] == "ok"
    assert "telemetry" in dashboard_payload
    assert dashboard_payload["telemetry"]["overall"]["requests"] >= 1
    assert isinstance(dashboard_payload["telemetry"]["modules"], list)
    assert any(item["module"] == "pipeline" for item in dashboard_payload["telemetry"]["modules"])

    report_response = client.post(
        "/api/v1/reports/comparison.docx",
        json={
            "title": "Отчет",
            "criteria": config["criteria"],
            "result": run_response.json(),
        },
    )
    assert report_response.status_code == 200
    assert report_response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
