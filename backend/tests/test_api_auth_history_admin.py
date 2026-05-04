import json


def test_register_conflict_and_admin_access_control(client):
    first_register = client.post(
        "/api/v1/auth/register",
        json={"email": "user@example.com", "full_name": "Test User", "password": "secret123"},
    )
    assert first_register.status_code == 201
    user_token = first_register.json()["access_token"]

    duplicate_register = client.post(
        "/api/v1/auth/register",
        json={"email": "user@example.com", "full_name": "Test User", "password": "secret123"},
    )
    assert duplicate_register.status_code == 409

    forbidden_admin_users = client.get("/api/v1/admin/users", headers={"Authorization": f"Bearer {user_token}"})
    assert forbidden_admin_users.status_code == 403

    admin_login = client.post(
        "/api/v1/auth/login",
        json={"email": "admin@example.com", "password": "admin12345"},
    )
    admin_token = admin_login.json()["access_token"]

    admin_users = client.get("/api/v1/admin/users", headers={"Authorization": f"Bearer {admin_token}"})
    assert admin_users.status_code == 200
    assert len(admin_users.json()) >= 2

    admin_stats = client.get("/api/v1/admin/stats", headers={"Authorization": f"Bearer {admin_token}"})
    assert admin_stats.status_code == 200
    assert admin_stats.json()["admins_total"] >= 1


def test_projects_history_and_storage_filters_work_together(client, sample_csv_bytes: bytes):
    upload_response = client.post(
        "/api/v1/files/upload?purpose=test-history",
        files={"file": ("dataset.csv", sample_csv_bytes, "text/csv")},
    )
    assert upload_response.status_code == 201
    dataset_file_id = upload_response.json()["id"]

    report_upload = client.post(
        "/api/v1/files/upload?purpose=test-report",
        files={"file": ("report.docx", b"report-bytes", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")},
    )
    assert report_upload.status_code == 201
    report_file_id = report_upload.json()["id"]

    project_response = client.post(
        "/api/v1/projects",
        json={"owner_user_id": 1, "owner_email": "admin@example.com", "name": "Project A"},
    )
    assert project_response.status_code == 201
    project_id = project_response.json()["id"]

    history_create = client.post(
        "/api/v1/comparison-history",
        json={
            "user_id": 1,
            "user_email": "admin@example.com",
            "title": "Run 1",
            "project_id": project_id,
            "dataset_file_id": dataset_file_id,
            "parameters_json": json.dumps({"mode": "rating"}),
            "summary_json": json.dumps({"best_object_id": "1"}),
        },
    )
    assert history_create.status_code == 201
    history_id = history_create.json()["id"]
    assert history_create.json()["version_number"] == 1

    history_create_2 = client.post(
        "/api/v1/comparison-history",
        json={
            "user_id": 1,
            "user_email": "admin@example.com",
            "title": "Run 2",
            "project_id": project_id,
            "dataset_file_id": dataset_file_id,
            "parent_history_id": history_id,
            "parameters_json": json.dumps({"mode": "analog_search"}),
            "summary_json": json.dumps({"best_object_id": "2"}),
        },
    )
    assert history_create_2.status_code == 201
    assert history_create_2.json()["version_number"] == 2

    patch_response = client.patch(
        f"/api/v1/comparison-history/{history_id}/result-file",
        json={"result_file_id": report_file_id},
    )
    assert patch_response.status_code == 200
    assert patch_response.json()["result_file_id"] == report_file_id

    filtered_history = client.get(f"/api/v1/comparison-history?project_id={project_id}")
    assert filtered_history.status_code == 200
    assert len(filtered_history.json()) == 2

    filtered_projects = client.get("/api/v1/projects?user_id=1")
    assert filtered_projects.status_code == 200
    assert len(filtered_projects.json()) == 1

    storage_stats = client.get("/api/v1/stats?user_id=1")
    assert storage_stats.status_code == 200
    payload = storage_stats.json()
    assert payload["projects_total"] == 1
    assert payload["comparisons_total"] == 2


def test_pipeline_raw_objects_returns_original_values(client, sample_csv_bytes: bytes):
    profile_response = client.post(
        "/api/v1/pipeline/upload-profile",
        files={"file": ("dataset.csv", sample_csv_bytes, "text/csv")},
    )
    assert profile_response.status_code == 200
    dataset_file_id = profile_response.json()["dataset_file_id"]

    raw_objects = client.post(
        "/api/v1/pipeline/raw-objects",
        json={"dataset_file_id": dataset_file_id, "filename": "dataset.csv", "object_ids": ["1", "3"]},
    )
    assert raw_objects.status_code == 200
    payload = raw_objects.json()
    assert payload["objects"]["1"]["price"] == "100"
    assert payload["objects"]["3"]["condition"] == "old"


def test_system_dashboard_shows_admin_stats_only_for_admin(client):
    anonymous_dashboard = client.get("/api/v1/system/dashboard")
    assert anonymous_dashboard.status_code == 200
    assert anonymous_dashboard.json()["auth"] is None

    login_response = client.post(
        "/api/v1/auth/login",
        json={"email": "admin@example.com", "password": "admin12345"},
    )
    token = login_response.json()["access_token"]

    admin_dashboard = client.get("/api/v1/system/dashboard", headers={"Authorization": f"Bearer {token}"})
    assert admin_dashboard.status_code == 200
    assert admin_dashboard.json()["auth"]["admins_total"] >= 1
