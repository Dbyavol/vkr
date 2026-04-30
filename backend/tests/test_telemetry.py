from app.core.telemetry import get_telemetry_snapshot, record_request, reset_telemetry


def test_telemetry_records_pipeline_and_system_modules():
    reset_telemetry()

    record_request("/api/v1/pipeline/run", 120.0, 200)
    record_request("/api/v1/pipeline/upload-profile", 80.0, 500)
    record_request("/api/v1/system/dashboard", 25.0, 200)

    snapshot = get_telemetry_snapshot()

    assert snapshot["overall"]["requests"] == 3
    assert snapshot["overall"]["errors"] == 1
    modules = {item["module"]: item for item in snapshot["modules"]}
    assert modules["pipeline"]["requests"] == 2
    assert modules["pipeline"]["errors"] == 1
    assert modules["pipeline"]["avg_ms"] > 0
    assert modules["system"]["requests"] == 1
