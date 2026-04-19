import base64

from app.services.import_parser import parse_dataset_base64, parse_dataset_bytes


def test_parse_csv_builds_normalized_dataset() -> None:
    body = (
        "Price,Area,Condition\n"
        "100,45,good\n"
        "200,50,excellent\n"
    ).encode("utf-8")
    result = parse_dataset_bytes("sample.csv", body)

    assert result.rows_total == 2
    assert result.columns[0].normalized_name == "price"
    assert result.normalized_dataset.rows[0].values["price"] in {"100", 100}


def test_parse_json_base64() -> None:
    content = base64.b64encode(b'[{"Name": "A", "Score": 1}, {"Name": "B", "Score": 2}]').decode("utf-8")
    result = parse_dataset_base64("sample.json", content)

    assert result.rows_total == 2
    assert any(column.normalized_name == "name" for column in result.columns)
