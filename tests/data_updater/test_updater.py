"""updater._extract_facility_from_s3_event / parse_daily_data のユニットテスト."""

import io
from datetime import datetime
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def updater_module():
    import updater  # noqa: PLC0415 — autouse fixture が sys.path を整えた後でロード
    return updater


# ----------------- _extract_facility_from_s3_event -----------------

def test_extract_facility_honmoku(updater_module):
    event = {"Records": [{"s3": {"object": {"key": "data/honmoku/2026-04-23/body.csv"}}}]}
    facility, target_date = updater_module._extract_facility_from_s3_event(event)
    assert facility == "honmoku"
    assert target_date == datetime(2026, 4, 23)


def test_extract_facility_daikoku(updater_module):
    event = {"Records": [{"s3": {"object": {"key": "data/daikoku/2025-12-31/head.csv"}}}]}
    facility, target_date = updater_module._extract_facility_from_s3_event(event)
    assert facility == "daikoku"
    assert target_date == datetime(2025, 12, 31)


def test_extract_facility_invalid_key_raises(updater_module):
    event = {"Records": [{"s3": {"object": {"key": "random/path/file.txt"}}}]}
    with pytest.raises(ValueError, match="S3キーから施設名を抽出できません"):
        updater_module._extract_facility_from_s3_event(event)


# ----------------- parse_daily_data -----------------

def _make_s3_client(responses: dict):
    """responses は {key: csv_text} の辞書. キーに無いものは NoSuchKey 例外を投げる."""
    s3 = MagicMock()
    s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    def _get_object(Bucket, Key):  # noqa: N803 (boto3 規約)
        if Key not in responses:
            raise s3.exceptions.NoSuchKey()
        return {"Body": io.BytesIO(responses[Key].encode("utf-8"))}

    s3.get_object.side_effect = _get_object
    return s3


def test_parse_daily_data_extracts_aji_count_and_water_temp(updater_module):
    head_csv = "天気,水温,来場者数\n晴,18.5℃,320\n"
    body_csv = "魚,合計\nアジ,1200\nサバ,50\n"
    s3 = _make_s3_client({
        "data/honmoku/2026-04-23/head.csv": head_csv,
        "data/honmoku/2026-04-23/body.csv": body_csv,
    })

    result = updater_module.parse_daily_data(s3, "bucket", "honmoku", datetime(2026, 4, 23))

    assert result is not None
    assert result["aji_count"] == 1200
    assert result["visitors"] == 320
    assert result["water_temp"] == 18.5
    assert result["weather"] == "晴"
    assert result["date"] == datetime(2026, 4, 23)


def test_parse_daily_data_falls_back_to_nyujosha_column(updater_module):
    # 「来場者数」ではなく「入場者数」列を使う施設にも対応する
    head_csv = "天気,水温,入場者数\n曇,15.2℃,210\n"
    body_csv = "魚,合計\nアジ,300\n"
    s3 = _make_s3_client({
        "data/daikoku/2026-04-23/head.csv": head_csv,
        "data/daikoku/2026-04-23/body.csv": body_csv,
    })

    result = updater_module.parse_daily_data(s3, "bucket", "daikoku", datetime(2026, 4, 23))
    assert result["visitors"] == 210
    assert result["aji_count"] == 300


def test_parse_daily_data_returns_zero_when_no_aji_row(updater_module):
    head_csv = "天気,水温,来場者数\n雨,12.0℃,100\n"
    body_csv = "魚,合計\nサバ,50\nイワシ,20\n"
    s3 = _make_s3_client({
        "data/honmoku/2026-04-23/head.csv": head_csv,
        "data/honmoku/2026-04-23/body.csv": body_csv,
    })

    result = updater_module.parse_daily_data(s3, "bucket", "honmoku", datetime(2026, 4, 23))
    assert result["aji_count"] == 0


def test_parse_daily_data_returns_none_when_head_missing(updater_module):
    s3 = _make_s3_client({})
    result = updater_module.parse_daily_data(s3, "bucket", "honmoku", datetime(2026, 4, 23))
    assert result is None


def test_parse_daily_data_returns_none_when_head_empty(updater_module):
    head_csv = "天気,水温,来場者数\n"
    s3 = _make_s3_client({"data/honmoku/2026-04-23/head.csv": head_csv})
    result = updater_module.parse_daily_data(s3, "bucket", "honmoku", datetime(2026, 4, 23))
    assert result is None
