from app.services.import_parser import parse_dataset_bytes


def test_parse_csv_normalizes_columns_and_builds_preview(sample_csv_bytes: bytes):
    preview = parse_dataset_bytes("dataset.csv", sample_csv_bytes)

    assert preview.rows_total == 3
    assert [column.normalized_name for column in preview.columns][:3] == ["price", "area", "rooms"]
    assert preview.preview_rows[0]["price"] == "100"
    assert preview.normalized_dataset.rows[1].values["condition"] == "good"


def test_parse_json_array_builds_rows():
    payload = b'[{"name":"A","score":10},{"name":"B","score":20}]'

    preview = parse_dataset_bytes("dataset.json", payload)

    assert preview.rows_total == 2
    assert preview.normalized_dataset.rows[0].values["name"] == "A"
    assert preview.columns[1].normalized_name == "score"
